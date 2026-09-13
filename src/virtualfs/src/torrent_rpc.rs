use common::AppError;
use fm_core::rpc::FileSystemRpc;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TorrentEntry {
    pub id: usize,
    pub path: String,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TorrentContents {
    pub name: String,
    pub entries: Vec<TorrentEntry>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TorrentView {
    #[default]
    All,
    Downloaded,
    Peers,
}

pub struct TorrentFileSystemRpc {
    pub relative_path_in_parent: String,
    pub parent_rpc: Rc<dyn FileSystemRpc>,
    pub contents: Rc<RefCell<Option<TorrentContents>>>,
    pub view: Rc<std::cell::Cell<TorrentView>>,
}

impl TorrentFileSystemRpc {
    pub fn new(relative_path_in_parent: String, parent_rpc: Rc<dyn FileSystemRpc>) -> Self {
        Self {
            relative_path_in_parent,
            parent_rpc,
            contents: Rc::new(RefCell::new(None)),
            view: Rc::new(std::cell::Cell::new(TorrentView::All)),
        }
    }

    pub fn torrent_path(&self) -> &str {
        &self.relative_path_in_parent
    }

    pub fn view(&self) -> TorrentView {
        self.view.get()
    }

    pub fn set_view(&self, view: TorrentView) {
        self.view.set(view);
    }
}

pub fn downloaded_rows(
    entries: &[TorrentEntry],
    is_complete: &dyn Fn(usize, u64) -> bool,
) -> Vec<fm_core::rpc::RemoteFileEntry> {
    let mut rows: Vec<fm_core::rpc::RemoteFileEntry> = entries
        .iter()
        .filter(|e| is_complete(e.id, e.size))
        .map(|e| fm_core::rpc::RemoteFileEntry {
            name: e.path.clone(),
            is_dir: false,
            size: e.size,
            modified: 0,
            permissions: None,
        })
        .collect();
    rows.sort_by(|a, b| a.name.cmp(&b.name));
    rows
}

pub fn peer_rows(peers: &[crate::torrent_session::PeerRow]) -> Vec<fm_core::rpc::RemoteFileEntry> {
    peers
        .iter()
        .map(|p| fm_core::rpc::RemoteFileEntry {
            name: p.addr.clone(),
            is_dir: false,
            size: p.fetched_bytes,
            modified: 0,
            permissions: None,
        })
        .collect()
}

pub fn parse_torrent(data: &[u8]) -> Result<TorrentContents, AppError> {
    let meta = librqbit::torrent_from_bytes::<librqbit::ByteBufOwned>(data)
        .map_err(|e| AppError::Other(format!("not a valid torrent file: {}", e)))?;
    let name = meta
        .info
        .name
        .as_ref()
        .map(|n| String::from_utf8_lossy(n.as_ref()).to_string())
        .unwrap_or_default();
    let details = meta
        .info
        .iter_file_details()
        .map_err(|e| AppError::Other(format!("torrent metadata is unusable: {}", e)))?;
    let mut entries = Vec::new();
    for (id, d) in details.enumerate() {
        if d.attrs().padding {
            continue;
        }
        let parts = d
            .filename
            .to_vec()
            .map_err(|e| AppError::Other(format!("torrent has an unreadable file name: {}", e)))?;
        let path = parts.join("/");
        if path.is_empty() {
            continue;
        }
        entries.push(TorrentEntry {
            id,
            path,
            size: d.len,
        });
    }
    Ok(TorrentContents { name, entries })
}

pub fn normalize_internal_path(path: &str) -> String {
    path.replace('\\', "/")
        .trim_matches('/')
        .to_string()
}

pub fn list_level(entries: &[TorrentEntry], internal_dir: &str) -> Vec<fm_core::rpc::RemoteFileEntry> {
    let dir = normalize_internal_path(internal_dir);
    let prefix = if dir.is_empty() {
        String::new()
    } else {
        format!("{}/", dir)
    };

    let mut dirs: Vec<String> = Vec::new();
    let mut files: Vec<(String, u64)> = Vec::new();

    for entry in entries {
        let rest = match entry.path.strip_prefix(prefix.as_str()) {
            Some(r) if !prefix.is_empty() => r,
            _ if prefix.is_empty() => entry.path.as_str(),
            _ => continue,
        };
        let mut parts = rest.splitn(2, '/');
        let head = match parts.next() {
            Some(h) if !h.is_empty() => h,
            _ => continue,
        };
        if parts.next().is_some() {
            if !dirs.iter().any(|d| d == head) {
                dirs.push(head.to_string());
            }
        } else if !files.iter().any(|(n, _)| n == head) {
            files.push((head.to_string(), entry.size));
        }
    }

    files.retain(|(n, _)| !dirs.iter().any(|d| d == n));

    let mut out: Vec<fm_core::rpc::RemoteFileEntry> = dirs
        .into_iter()
        .map(|name| fm_core::rpc::RemoteFileEntry {
            name,
            is_dir: true,
            size: 0,
            modified: 0,
            permissions: Some(0o755),
        })
        .chain(files.into_iter().map(|(name, size)| fm_core::rpc::RemoteFileEntry {
            name,
            is_dir: false,
            size,
            modified: 0,
            permissions: None,
        }))
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

impl TorrentFileSystemRpc {
    async fn ensure_loaded(
        contents_cell: Rc<RefCell<Option<TorrentContents>>>,
        parent_rpc: Rc<dyn FileSystemRpc>,
        rel_path: String,
    ) -> Result<TorrentContents, AppError> {
        if let Some(cached) = contents_cell.borrow().clone() {
            return Ok(cached);
        }
        let data = parent_rpc.read_file(rel_path, None).await?;
        let parsed = tokio::task::spawn_blocking(move || parse_torrent(&data))
            .await
            .map_err(|e| AppError::Other(e.to_string()))??;
        *contents_cell.borrow_mut() = Some(parsed.clone());
        Ok(parsed)
    }

    pub fn torrent_name(&self) -> Option<String> {
        self.contents.borrow().as_ref().map(|c| c.name.clone())
    }
}

#[async_trait::async_trait(?Send)]
impl FileSystemRpc for TorrentFileSystemRpc {
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }

    async fn list_dir(&self, path: String) -> Result<Vec<fm_core::rpc::RemoteFileEntry>, AppError> {
        let contents = Self::ensure_loaded(
            self.contents.clone(),
            self.parent_rpc.clone(),
            self.relative_path_in_parent.clone(),
        )
        .await?;
        match self.view.get() {
            TorrentView::All => Ok(list_level(&contents.entries, &path)),
            TorrentView::Downloaded => {
                let key = self.relative_path_in_parent.clone();
                Ok(downloaded_rows(&contents.entries, &|id, size| {
                    crate::torrent_session::is_file_complete(&key, id, size)
                }))
            }
            TorrentView::Peers => Ok(peer_rows(&crate::torrent_session::peers(
                &self.relative_path_in_parent,
            ))),
        }
    }

    async fn read_file(
        &self,
        path: String,
        _progress_callback: Option<Box<dyn Fn(u64) + 'static>>,
    ) -> Result<Vec<u8>, AppError> {
        let wanted = normalize_internal_path(&path);
        if self.view.get() == TorrentView::Peers {
            let peers = crate::torrent_session::peers(&self.relative_path_in_parent);
            return peers
                .iter()
                .find(|p| p.addr == wanted)
                .map(|p| crate::torrent_session::peer_card(p).into_bytes())
                .ok_or_else(|| AppError::Other("this peer is no longer connected".to_string()));
        }
        let contents = Self::ensure_loaded(
            self.contents.clone(),
            self.parent_rpc.clone(),
            self.relative_path_in_parent.clone(),
        )
        .await?;
        let entry = contents
            .entries
            .iter()
            .find(|e| e.path == wanted)
            .ok_or_else(|| AppError::Other("no such file in this torrent".to_string()))?;
        if !crate::torrent_session::is_file_complete(
            &self.relative_path_in_parent,
            entry.id,
            entry.size,
        ) {
            return Err(AppError::Other(
                "this file has not been downloaded yet".to_string(),
            ));
        }
        let on_disk = crate::torrent_session::downloaded_file_path(
            &self.relative_path_in_parent,
            &entry.path,
        );
        std::fs::read(on_disk).map_err(AppError::from)
    }

    async fn create_directory(
        &self,
        _parent_path: String,
        _dir_name: String,
        _permissions: Option<u32>,
    ) -> Result<(), AppError> {
        Err(AppError::Other("Torrent filesystem is read-only".to_string()))
    }

    async fn delete_entries(&self, _paths: Vec<String>) -> Result<(), AppError> {
        Err(AppError::Other("Torrent filesystem is read-only".to_string()))
    }

    async fn rename_entry(&self, _path: String, _new_path: String) -> Result<(), AppError> {
        Err(AppError::Other("Torrent filesystem is read-only".to_string()))
    }

    async fn write_file(
        &self,
        _path: String,
        _content: Vec<u8>,
        _permissions: Option<u32>,
        _progress_callback: Option<Box<dyn Fn(u64) + 'static>>,
    ) -> Result<(), AppError> {
        Err(AppError::Other("Torrent filesystem is read-only".to_string()))
    }

    fn is_read_only(&self) -> bool {
        true
    }

    fn is_local(&self) -> bool {
        false
    }

    fn get_icon(&self, path: &str) -> String {
        if path.is_empty() || path == "/" {
            "folder".to_string()
        } else {
            "folder".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(paths: &[(&str, u64)]) -> Vec<TorrentEntry> {
        paths
            .iter()
            .enumerate()
            .map(|(id, (p, s))| TorrentEntry {
                id,
                path: (*p).to_string(),
                size: *s,
            })
            .collect()
    }

    #[test]
    fn a_single_file_torrent_lists_that_file_at_the_root() {
        let e = entries(&[("ubuntu.iso", 4096)]);
        let listing = list_level(&e, "");
        assert_eq!(listing.len(), 1);
        assert_eq!(listing[0].name, "ubuntu.iso");
        assert!(!listing[0].is_dir);
        assert_eq!(listing[0].size, 4096);
    }

    #[test]
    fn nested_paths_become_directories_at_the_level_above() {
        let e = entries(&[("disc/track1.flac", 10), ("disc/track2.flac", 20), ("readme.txt", 5)]);
        let listing = list_level(&e, "");
        let names: Vec<&str> = listing.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, vec!["disc", "readme.txt"]);
        assert!(listing[0].is_dir);
        assert_eq!(listing[0].size, 0);
    }

    #[test]
    fn descending_into_a_directory_lists_its_own_children() {
        let e = entries(&[("disc/track1.flac", 10), ("disc/art/cover.jpg", 7)]);
        let listing = list_level(&e, "disc");
        let names: Vec<&str> = listing.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, vec!["art", "track1.flac"]);
        assert!(listing[0].is_dir);
        assert!(!listing[1].is_dir);
        assert_eq!(listing[1].size, 10);
    }

    #[test]
    fn a_deeper_level_is_reached_through_its_full_prefix() {
        let e = entries(&[("disc/art/cover.jpg", 7), ("disc/art/back.jpg", 9)]);
        let listing = list_level(&e, "disc/art");
        let names: Vec<&str> = listing.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, vec!["back.jpg", "cover.jpg"]);
    }

    #[test]
    fn a_sibling_directory_does_not_leak_into_the_listing() {
        let e = entries(&[("a/one.bin", 1), ("ab/two.bin", 2)]);
        let listing = list_level(&e, "a");
        let names: Vec<&str> = listing.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, vec!["one.bin"]);
    }

    #[test]
    fn a_path_is_normalized_before_it_is_matched() {
        let e = entries(&[("disc/track1.flac", 10)]);
        assert_eq!(list_level(&e, "/disc/").len(), 1);
        assert_eq!(list_level(&e, "\\disc").len(), 1);
    }

    #[test]
    fn an_unknown_directory_lists_nothing() {
        let e = entries(&[("disc/track1.flac", 10)]);
        assert!(list_level(&e, "missing").is_empty());
    }

    #[test]
    fn garbage_is_not_mistaken_for_a_torrent() {
        assert!(parse_torrent(b"not a torrent at all").is_err());
        assert!(parse_torrent(b"").is_err());
    }
    fn single_file_torrent_bytes() -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(b"d8:announce3:abc4:infod6:lengthi1024e4:name8:test.bin12:piece lengthi16384e6:pieces20:");
        v.extend_from_slice(&[0u8; 20]);
        v.extend_from_slice(b"ee");
        v
    }

    fn multi_file_torrent_bytes() -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(b"d8:announce3:abc4:infod5:filesld6:lengthi10e4:pathl4:disc11:track1.flaceed6:lengthi20e4:pathl9:cover.jpgeee4:name5:album12:piece lengthi16384e6:pieces20:");
        v.extend_from_slice(&[0u8; 20]);
        v.extend_from_slice(b"ee");
        v
    }

    #[test]
    fn a_real_single_file_torrent_parses_to_one_entry() {
        let c = parse_torrent(&single_file_torrent_bytes()).unwrap();
        assert_eq!(c.name, "test.bin");
        assert_eq!(
            c.entries,
            vec![TorrentEntry {
                id: 0,
                path: "test.bin".to_string(),
                size: 1024
            }]
        );
    }

    #[test]
    fn a_real_multi_file_torrent_keeps_its_directory_structure() {
        let c = parse_torrent(&multi_file_torrent_bytes()).unwrap();
        assert_eq!(c.name, "album");
        assert_eq!(c.entries.len(), 2);
        assert_eq!(c.entries[0].path, "disc/track1.flac");
        assert_eq!(c.entries[0].size, 10);
        assert_eq!(c.entries[1].path, "cover.jpg");
    }

    #[test]
    fn a_parsed_multi_file_torrent_lists_like_a_directory_tree() {
        let c = parse_torrent(&multi_file_torrent_bytes()).unwrap();
        let root: Vec<String> = list_level(&c.entries, "")
            .into_iter()
            .map(|r| r.name)
            .collect();
        assert_eq!(root, vec!["cover.jpg".to_string(), "disc".to_string()]);
        let inner = list_level(&c.entries, "disc");
        assert_eq!(inner.len(), 1);
        assert_eq!(inner[0].name, "track1.flac");
        assert_eq!(inner[0].size, 10);
    }

    #[test]
    #[ignore]
    fn a_torrent_file_from_disk_parses() {
        let path = match std::env::var("IC_TORRENT_FILE") {
            Ok(p) => p,
            Err(_) => return,
        };
        let bytes = std::fs::read(&path).expect("torrent file is readable");
        let c = parse_torrent(&bytes).expect("torrent file parses");
        println!("name: {}", c.name);
        println!("files: {}", c.entries.len());
        for e in &c.entries {
            println!("  {} ({} bytes)", e.path, e.size);
        }
        let root = list_level(&c.entries, "");
        println!("root listing: {} rows", root.len());
        for r in &root {
            println!("  {} dir={} size={}", r.name, r.is_dir, r.size);
        }
        assert!(!c.entries.is_empty());
    }

    fn torrent_with_a_padding_file_bytes() -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(b"d8:announce3:abc4:infod5:filesl");
        v.extend_from_slice(b"d6:lengthi10e4:pathl4:disc11:track1.flacee");
        v.extend_from_slice(b"d4:attr1:p6:lengthi5e4:pathl6:.pad_5ee");
        v.extend_from_slice(b"d6:lengthi20e4:pathl9:cover.jpgee");
        v.extend_from_slice(b"e4:name5:album12:piece lengthi16384e6:pieces20:");
        v.extend_from_slice(&[0u8; 20]);
        v.extend_from_slice(b"ee");
        v
    }

    #[test]
    fn a_padding_file_is_hidden_but_still_consumes_its_index() {
        let c = parse_torrent(&torrent_with_a_padding_file_bytes()).unwrap();
        assert_eq!(c.entries.len(), 2);
        assert_eq!(c.entries[0].path, "disc/track1.flac");
        assert_eq!(c.entries[0].id, 0);
        assert_eq!(c.entries[1].path, "cover.jpg");
        assert_eq!(
            c.entries[1].id, 2,
            "the padding file at index 1 must keep its slot, or only_files would select the wrong file"
        );
    }

}
