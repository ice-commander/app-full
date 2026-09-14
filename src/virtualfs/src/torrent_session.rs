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
    refs: HashMap<String, usize>,
    runtime: Option<tokio::runtime::Handle>,
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
    guard.runtime = tokio::runtime::Handle::try_current().ok();
    guard.session = Some(session.clone());
    Ok(session)
}

const STOP_ATTEMPTS: usize = 30;
const STOP_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(300);

pub const CLOSE_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

pub fn acquire(torrent_path: &str) {
    *lock().refs.entry(torrent_path.to_string()).or_insert(0) += 1;
}

pub fn open_count(torrent_path: &str) -> usize {
    lock().refs.get(torrent_path).copied().unwrap_or(0)
}

pub fn release(torrent_path: &str) {
    let now_unused = {
        let mut guard = lock();
        match guard.refs.get_mut(torrent_path) {
            Some(n) => {
                *n = n.saturating_sub(1);
                let empty = *n == 0;
                if empty {
                    guard.refs.remove(torrent_path);
                }
                empty
            }
            None => false,
        }
    };
    if !now_unused {
        return;
    }
    let handle = lock().runtime.clone();
    let Some(handle) = handle else {
        return;
    };
    let key = torrent_path.to_string();
    handle.spawn(async move {
        tokio::time::sleep(CLOSE_GRACE).await;
        let pending = {
            let mut guard = lock();
            if guard.refs.contains_key(&key) {
                None
            } else {
                guard
                    .torrents
                    .remove(&key)
                    .map(|m| (m.id, guard.session.clone()))
            }
        };
        if let Some((id, Some(session))) = pending {
            let _ = session.delete(id.into(), false).await;
        }
    });
}

pub async fn shutdown() {
    let session = {
        let mut guard = lock();
        guard.torrents.clear();
        guard.refs.clear();
        guard.session.take()
    };
    if let Some(s) = session {
        s.stop().await;
    }
}

pub const SEEDING_KEY: &str = "torrent.seed";

static SEEDING: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);

pub fn seeding_enabled() -> bool {
    SEEDING.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn set_seeding_enabled(enabled: bool) {
    SEEDING.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

pub fn is_open(torrent_path: &str) -> bool {
    lock().torrents.contains_key(torrent_path)
}

pub fn is_active(torrent_path: &str) -> bool {
    matches!(
        stats(torrent_path).map(|s| s.state),
        Some(librqbit::TorrentStatsState::Live) | Some(librqbit::TorrentStatsState::Initializing)
    )
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
    let Some(s) = session else {
        return Ok(());
    };
    let mut last = None;
    for _ in 0..STOP_ATTEMPTS {
        if matches!(handle.stats().state, librqbit::TorrentStatsState::Paused) {
            return Ok(());
        }
        match s.pause(&handle).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                last = Some(e.to_string());
                tokio::time::sleep(STOP_RETRY_DELAY).await;
            }
        }
    }
    Err(AppError::Other(format!(
        "could not pause the torrent: {}",
        last.unwrap_or_else(|| "unknown reason".to_string())
    )))
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

#[derive(Clone, Debug, PartialEq)]
pub struct Activity {
    pub torrents: usize,
    pub download_mbps: f64,
    pub upload_mbps: f64,
}

pub fn format_speed(mbps: f64) -> String {
    if mbps >= 1.0 {
        format!("{:.1} MB/s", mbps)
    } else if mbps > 0.0 {
        format!("{:.0} KB/s", (mbps * 1024.0).max(1.0))
    } else {
        "0".to_string()
    }
}

pub fn activity() -> Option<Activity> {
    let handles: Vec<Arc<librqbit::ManagedTorrent>> =
        lock().torrents.values().map(|m| m.handle.clone()).collect();
    let mut download_mbps = 0.0;
    let mut upload_mbps = 0.0;
    let mut active = 0usize;
    for h in &handles {
        let stats = h.stats();
        if !matches!(
            stats.state,
            librqbit::TorrentStatsState::Live | librqbit::TorrentStatsState::Initializing
        ) {
            continue;
        }
        active += 1;
        if let Some(live) = stats.live {
            download_mbps += live.download_speed.mbps;
            upload_mbps += live.upload_speed.mbps;
        }
    }
    if active == 0 {
        return None;
    }
    Some(Activity {
        torrents: active,
        download_mbps,
        upload_mbps,
    })
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

pub fn shutdown_blocking(timeout: std::time::Duration) {
    let handle = lock().runtime.clone();
    let Some(handle) = handle else {
        return;
    };
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        handle.block_on(shutdown());
        let _ = tx.send(());
    });
    let _ = rx.recv_timeout(timeout);
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
        assert!(!is_open("/nowhere/none.torrent"));
        assert!(!is_active("/nowhere/none.torrent"));
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
            assert!(is_active(&copy_s), "a freshly started torrent is actually running");

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

    #[test]
    fn opening_the_same_torrent_twice_keeps_it_alive_until_both_are_gone() {
        let key = "/tmp/refcount-probe.torrent";
        assert_eq!(open_count(key), 0);
        acquire(key);
        assert_eq!(open_count(key), 1);
        acquire(key);
        assert_eq!(open_count(key), 2);
        release(key);
        assert_eq!(open_count(key), 1);
        release(key);
        assert_eq!(open_count(key), 0);
    }

    #[test]
    fn releasing_a_torrent_nobody_opened_is_harmless() {
        release("/tmp/never-opened.torrent");
        assert_eq!(open_count("/tmp/never-opened.torrent"), 0);
    }

    #[test]
    fn two_torrents_count_independently() {
        let a = "/tmp/ref-a.torrent";
        let b = "/tmp/ref-b.torrent";
        acquire(a);
        acquire(b);
        acquire(b);
        release(a);
        assert_eq!(open_count(a), 0);
        assert_eq!(open_count(b), 2);
        release(b);
        release(b);
        assert_eq!(open_count(b), 0);
    }

    #[test]
    #[ignore]
    fn a_torrent_closes_only_when_the_last_panel_leaves_it() {
        let path = match std::env::var("IC_TORRENT_FILE") {
            Ok(p) => p,
            Err(_) => return,
        };
        let dir = tempfile::tempdir().unwrap();
        let copy = dir.path().join("refs.torrent");
        std::fs::copy(&path, &copy).unwrap();
        let key = copy.to_string_lossy().to_string();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            acquire(&key);
            acquire(&key);
            start(&key, std::fs::read(&copy).unwrap(), Some(vec![0]))
                .await
                .expect("torrent starts");
            assert!(is_open(&key));

            release(&key);
            tokio::time::sleep(CLOSE_GRACE + std::time::Duration::from_secs(1)).await;
            assert!(
                is_open(&key),
                "one panel left, the other still has it open"
            );

            release(&key);
            tokio::time::sleep(CLOSE_GRACE + std::time::Duration::from_secs(2)).await;
            assert!(!is_open(&key), "the last panel left, so it closes");

            assert!(
                data_dir_for(&key).exists(),
                "closing keeps the downloaded data on disk"
            );
        });
    }

    #[test]
    #[ignore]
    fn a_brief_dip_to_zero_during_navigation_does_not_close_a_torrent() {
        let path = match std::env::var("IC_TORRENT_FILE") {
            Ok(p) => p,
            Err(_) => return,
        };
        let dir = tempfile::tempdir().unwrap();
        let copy = dir.path().join("dip.torrent");
        std::fs::copy(&path, &copy).unwrap();
        let key = copy.to_string_lossy().to_string();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            acquire(&key);
            start(&key, std::fs::read(&copy).unwrap(), Some(vec![0]))
                .await
                .expect("torrent starts");

            release(&key);
            acquire(&key);
            tokio::time::sleep(CLOSE_GRACE + std::time::Duration::from_secs(1)).await;
            assert!(
                is_open(&key),
                "the provider was rebuilt during navigation, not abandoned"
            );
            release(&key);
        });
    }

    #[test]
    fn a_speed_is_written_the_way_a_person_reads_it() {
        assert_eq!(format_speed(0.0), "0");
        assert_eq!(format_speed(2.5), "2.5 MB/s");
        assert_eq!(format_speed(0.5), "512 KB/s");
    }

    #[test]
    fn a_trickle_never_rounds_down_to_nothing() {
        assert_eq!(format_speed(0.0001), "1 KB/s");
    }

    #[test]
    fn nothing_open_means_no_activity_to_show() {
        assert!(activity().is_none());
    }

    fn offline_torrent_bytes() -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(b"d8:announce27:http://127.0.0.1:1/announce4:infod6:lengthi16384e4:name9:probe.bin12:piece lengthi16384e6:pieces20:");
        v.extend_from_slice(&[0u8; 20]);
        v.extend_from_slice(b"ee");
        v
    }

    #[test]
    #[ignore]
    fn stopping_a_torrent_makes_the_activity_indicator_go_away() {
        let dir = tempfile::tempdir().unwrap();
        let copy = dir.path().join("stopme.torrent");
        std::fs::write(&copy, offline_torrent_bytes()).unwrap();
        let key = copy.to_string_lossy().to_string();

        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            acquire(&key);
            start(&key, std::fs::read(&copy).unwrap(), None)
                .await
                .expect("torrent starts");
            for _ in 0..20 {
                if is_active(&key) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            assert!(is_active(&key), "it is running before we stop it");
            assert!(activity().is_some(), "the header shows something while it runs");

            stop(&key).await.expect("torrent pauses");
            for _ in 0..20 {
                if !is_active(&key) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
            assert!(!is_active(&key), "stop really pauses the torrent");
            assert!(
                activity().is_none(),
                "a paused torrent must not keep the header indicator alive"
            );
            assert!(is_open(&key), "it stays open, it is only paused");
            release(&key);
        });
    }

}
