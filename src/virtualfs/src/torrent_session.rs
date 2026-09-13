use common::AppError;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TorrentState {
    pub info_hash: String,
    pub selected: Vec<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerRow {
    pub addr: String,
    pub state: String,
    pub fetched_bytes: u64,
    pub pieces: u32,
    pub errors: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileProgress {
    pub index: usize,
    pub done_bytes: u64,
    pub total_bytes: u64,
}

impl FileProgress {
    pub fn is_complete(&self) -> bool {
        self.total_bytes > 0 && self.done_bytes >= self.total_bytes
    }
}

pub fn data_dir_for(torrent_path: &str) -> PathBuf {
    PathBuf::from(format!("{}-data", torrent_path))
}

pub fn state_file_for(torrent_path: &str) -> PathBuf {
    PathBuf::from(format!("{}-state.json", torrent_path))
}

pub fn read_state(torrent_path: &str) -> Option<TorrentState> {
    let bytes = std::fs::read(state_file_for(torrent_path)).ok()?;
    serde_json::from_slice(&bytes).ok()
}

pub fn write_state(torrent_path: &str, state: &TorrentState) -> Result<(), AppError> {
    let target = state_file_for(torrent_path);
    let tmp = target.with_extension("json.tmp");
    let body = serde_json::to_vec_pretty(state).map_err(|e| AppError::Other(e.to_string()))?;
    std::fs::write(&tmp, body).map_err(AppError::from)?;
    std::fs::rename(&tmp, &target).map_err(AppError::from)
}

struct Managed {
    id: usize,
    handle: Arc<librqbit::ManagedTorrent>,
}

#[derive(Default)]
struct Engine {
    session: Option<Arc<librqbit::Session>>,
    torrents: HashMap<String, Managed>,
}

fn engine() -> &'static Mutex<Engine> {
    static ENGINE: OnceLock<Mutex<Engine>> = OnceLock::new();
    ENGINE.get_or_init(|| Mutex::new(Engine::default()))
}

fn lock() -> std::sync::MutexGuard<'static, Engine> {
    engine().lock().unwrap_or_else(|e| e.into_inner())
}

async fn ensure_session() -> Result<Arc<librqbit::Session>, AppError> {
    if let Some(s) = lock().session.clone() {
        return Ok(s);
    }
    let opts = librqbit::SessionOptions {
        disable_dht_persistence: true,
        persistence: None,
        disable_upload: !seeding_enabled(),
        ..Default::default()
    };
    let session = librqbit::Session::new_with_opts(std::env::temp_dir(), opts)
        .await
        .map_err(|e| AppError::Other(format!("could not start the torrent session: {}", e)))?;
    let mut guard = lock();
    if let Some(existing) = guard.session.clone() {
        return Ok(existing);
    }
    guard.session = Some(session.clone());
    Ok(session)
}

pub const SEEDING_KEY: &str = "torrent.seed";

static SEEDING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

pub fn seeding_enabled() -> bool {
    SEEDING.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn set_seeding_enabled(enabled: bool) {
    SEEDING.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

pub fn is_running(torrent_path: &str) -> bool {
    lock().torrents.contains_key(torrent_path)
}

pub async fn start(
    torrent_path: &str,
    bytes: Vec<u8>,
    selected: Option<Vec<usize>>,
) -> Result<(), AppError> {
    if let Some(m) = lock().torrents.get(torrent_path) {
        let handle = m.handle.clone();
        let session = lock().session.clone();
        if let Some(s) = session {
            return s
                .unpause(&handle)
                .await
                .map_err(|e| AppError::Other(format!("could not resume the torrent: {}", e)));
        }
        return Ok(());
    }

    let session = ensure_session().await?;
    let output = data_dir_for(torrent_path);
    std::fs::create_dir_all(&output).map_err(AppError::from)?;

    let opts = librqbit::AddTorrentOptions {
        paused: false,
        overwrite: true,
        only_files: selected.clone(),
        output_folder: Some(output.to_string_lossy().to_string()),
        ..Default::default()
    };
    let added = session
        .add_torrent(
            librqbit::AddTorrent::TorrentFileBytes(bytes.into()),
            Some(opts),
        )
        .await
        .map_err(|e| AppError::Other(format!("could not add the torrent: {}", e)))?;

    let (id, handle) = match added {
        librqbit::AddTorrentResponse::Added(id, h) => (id, h),
        librqbit::AddTorrentResponse::AlreadyManaged(..) => {
            return Err(AppError::Other(
                "this torrent is already open somewhere else in the session".to_string(),
            ))
        }
        librqbit::AddTorrentResponse::ListOnly(_) => {
            return Err(AppError::Other("the torrent was not added".to_string()))
        }
    };

    let info_hash = handle.info_hash().as_string();
    lock().torrents.insert(
        torrent_path.to_string(),
        Managed {
            id,
            handle: handle.clone(),
        },
    );

    let _ = write_state(
        torrent_path,
        &TorrentState {
            info_hash,
            selected: selected.unwrap_or_default(),
        },
    );

    Ok(())
}

pub async fn stop(torrent_path: &str) -> Result<(), AppError> {
    let (session, handle) = {
        let guard = lock();
        match guard.torrents.get(torrent_path) {
            Some(m) => (guard.session.clone(), m.handle.clone()),
            None => return Ok(()),
        }
    };
    if let Some(s) = session {
        s.pause(&handle)
            .await
            .map_err(|e| AppError::Other(format!("could not pause the torrent: {}", e)))?;
    }
    Ok(())
}

pub async fn set_selection(torrent_path: &str, selected: &HashSet<usize>) -> Result<(), AppError> {
    let found = {
        let guard = lock();
        guard
            .torrents
            .get(torrent_path)
            .map(|m| (guard.session.clone(), m.handle.clone()))
    };
    let (session, handle) = match found {
        Some(v) => v,
        None => return Err(AppError::Other("this torrent is not running".to_string())),
    };
    if let Some(s) = session {
        s.update_only_files(&handle, selected)
            .await
            .map_err(|e| AppError::Other(format!("could not change the file selection: {}", e)))?;
    }
    Ok(())
}

pub async fn cleanup(torrent_path: &str, delete_torrent_file: bool) -> Result<(), AppError> {
    let entry = { lock().torrents.remove(torrent_path) };
    if let Some(m) = entry {
        let session = lock().session.clone();
        if let Some(s) = session {
            let _ = s.delete(m.id.into(), true).await;
        }
    }
    let data = data_dir_for(torrent_path);
    if data.exists() {
        std::fs::remove_dir_all(&data).map_err(AppError::from)?;
    }
    let state = state_file_for(torrent_path);
    if state.exists() {
        std::fs::remove_file(&state).map_err(AppError::from)?;
    }
    if delete_torrent_file {
        let t = PathBuf::from(torrent_path);
        if t.exists() {
            std::fs::remove_file(&t).map_err(AppError::from)?;
        }
    }
    Ok(())
}

pub fn peers(torrent_path: &str) -> Vec<PeerRow> {
    let handle = match lock().torrents.get(torrent_path).map(|m| m.handle.clone()) {
        Some(h) => h,
        None => return Vec::new(),
    };
    let mut rows = handle.with_state(|state| match state {
        librqbit::ManagedTorrentState::Live(live) => live
            .per_peer_stats_snapshot(Default::default())
            .peers
            .into_iter()
            .map(|(addr, s)| PeerRow {
                addr,
                state: s.state.to_string(),
                fetched_bytes: s.counters.fetched_bytes,
                pieces: s.counters.downloaded_and_checked_pieces,
                errors: s.counters.errors,
            })
            .collect(),
        _ => Vec::new(),
    });
    rows.sort_by(|a: &PeerRow, b: &PeerRow| a.addr.cmp(&b.addr));
    rows
}

pub fn peer_card(row: &PeerRow) -> String {
    format!(
        "address: {}\nstate: {}\nfetched bytes: {}\nchecked pieces: {}\nerrors: {}\n",
        row.addr, row.state, row.fetched_bytes, row.pieces, row.errors
    )
}

pub fn downloaded_file_path(torrent_path: &str, file_path: &str) -> PathBuf {
    data_dir_for(torrent_path).join(file_path)
}

pub fn stats(torrent_path: &str) -> Option<librqbit::TorrentStats> {
    lock().torrents.get(torrent_path).map(|m| m.handle.stats())
}

pub fn file_progress(torrent_path: &str, sizes: &[u64]) -> Vec<FileProgress> {
    let stats = match stats(torrent_path) {
        Some(s) => s,
        None => return Vec::new(),
    };
    stats
        .file_progress
        .iter()
        .enumerate()
        .map(|(index, done)| FileProgress {
            index,
            done_bytes: *done,
            total_bytes: sizes.get(index).copied().unwrap_or(0),
        })
        .collect()
}

pub fn is_file_complete(torrent_path: &str, index: usize, size: u64) -> bool {
    match stats(torrent_path) {
        Some(s) => s.file_progress.get(index).map(|d| *d >= size).unwrap_or(false),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn the_data_folder_sits_next_to_the_torrent() {
        assert_eq!(
            data_dir_for("/home/u/Downloads/Big Buck Bunny.torrent"),
            PathBuf::from("/home/u/Downloads/Big Buck Bunny.torrent-data")
        );
    }

    #[test]
    fn the_state_file_sits_next_to_the_torrent_not_inside_the_folder() {
        let state = state_file_for("/home/u/Downloads/Big Buck Bunny.torrent");
        assert_eq!(
            state,
            PathBuf::from("/home/u/Downloads/Big Buck Bunny.torrent-state.json")
        );
        assert_eq!(
            state_file_for("/x/Foo.TORRENT"),
            PathBuf::from("/x/Foo.TORRENT-state.json")
        );
        assert_eq!(state.parent(), Some(Path::new("/home/u/Downloads")));
        let data = data_dir_for("/home/u/Downloads/Big Buck Bunny.torrent");
        assert!(!state.starts_with(&data));
    }

    #[test]
    fn a_torrent_in_the_current_directory_still_resolves() {
        assert_eq!(data_dir_for("a.torrent"), PathBuf::from("a.torrent-data"));
        assert_eq!(state_file_for("a.torrent"), PathBuf::from("a.torrent-state.json"));
    }

    #[test]
    fn state_survives_a_write_and_read_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let torrent = dir.path().join("x.torrent");
        let path = torrent.to_string_lossy().to_string();
        let state = TorrentState {
            info_hash: "abc".to_string(),
            selected: vec![0, 2, 5],
        };
        write_state(&path, &state).unwrap();
        assert_eq!(read_state(&path), Some(state));
    }

    #[test]
    fn a_missing_or_corrupt_state_file_reads_as_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let torrent = dir.path().join("y.torrent");
        let path = torrent.to_string_lossy().to_string();
        assert_eq!(read_state(&path), None);
        std::fs::write(state_file_for(&path), b"{ not json").unwrap();
        assert_eq!(read_state(&path), None);
    }

    #[test]
    fn a_file_is_complete_only_when_every_byte_is_there() {
        let p = FileProgress {
            index: 0,
            done_bytes: 10,
            total_bytes: 10,
        };
        assert!(p.is_complete());
        let p = FileProgress {
            index: 0,
            done_bytes: 9,
            total_bytes: 10,
        };
        assert!(!p.is_complete());
    }

    #[test]
    fn a_torrent_that_never_started_reports_nothing() {
        assert!(!is_running("/nowhere/none.torrent"));
        assert!(stats("/nowhere/none.torrent").is_none());
        assert!(file_progress("/nowhere/none.torrent", &[1, 2]).is_empty());
        assert!(!is_file_complete("/nowhere/none.torrent", 0, 10));
    }
    #[test]
    #[ignore]
    fn a_real_torrent_downloads_into_the_data_folder() {
        let path = match std::env::var("IC_TORRENT_FILE") {
            Ok(p) => p,
            Err(_) => return,
        };
        let dir = tempfile::tempdir().unwrap();
        let copy = dir.path().join("bbb.torrent");
        std::fs::copy(&path, &copy).unwrap();
        let copy_s = copy.to_string_lossy().to_string();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            start(&copy_s, std::fs::read(&copy).unwrap(), Some(vec![0, 2]))
                .await
                .expect("torrent starts");
            assert!(is_running(&copy_s));

            let mut progressed = 0u64;
            for _ in 0..60 {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                if let Some(s) = stats(&copy_s) {
                    progressed = s.progress_bytes;
                    println!(
                        "state={:?} progress={} / {} peers_live={}",
                        s.state,
                        s.progress_bytes,
                        s.total_bytes,
                        s.live.is_some()
                    );
                    if progressed > 0 {
                        break;
                    }
                }
            }
            assert!(progressed > 0, "no bytes arrived in 60s");

            fn walk(dir: &Path, depth: usize) {
                if let Ok(rd) = std::fs::read_dir(dir) {
                    for e in rd.flatten() {
                        let p = e.path();
                        let len = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
                        println!("{}{} ({} bytes)", "  ".repeat(depth), p.display(), len);
                        if p.is_dir() {
                            walk(&p, depth + 1);
                        }
                    }
                }
            }
            println!("--- data dir layout:");
            walk(&data_dir_for(&copy_s), 1);
            assert!(
                data_dir_for(&copy_s).join("poster.jpg").exists(),
                "files land flat in the data folder, with no sub-folder named after the torrent"
            );

            stop(&copy_s).await.expect("torrent pauses");
            assert!(data_dir_for(&copy_s).exists(), "data folder was created");
            assert!(read_state(&copy_s).is_some(), "state file was written");

            cleanup(&copy_s, false).await.expect("cleanup runs");
            assert!(!data_dir_for(&copy_s).exists(), "data folder is gone");
            assert!(read_state(&copy_s).is_none(), "state file is gone");
            assert!(copy.exists(), "the .torrent itself survives without the checkbox");
        });
    }

    #[test]
    fn seeding_is_on_until_someone_turns_it_off() {
        assert!(seeding_enabled());
        set_seeding_enabled(false);
        assert!(!seeding_enabled());
        set_seeding_enabled(true);
        assert!(seeding_enabled());
    }

}
