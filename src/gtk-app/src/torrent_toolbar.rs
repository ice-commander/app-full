use adw::prelude::*;
use std::rc::Rc;
use virtualfs::torrent_rpc::{TorrentFileSystemRpc, TorrentView};

#[derive(Clone)]
pub struct TorrentToolbar {
    pub container: gtk::Box,
    all_btn: gtk::ToggleButton,
    downloaded_btn: gtk::ToggleButton,
    peers_btn: gtk::ToggleButton,
    start_btn: gtk::Button,
    stop_btn: gtk::Button,
}

fn with_torrent<R>(
    router: &panel_router::PanelRouter,
    f: impl FnOnce(&TorrentFileSystemRpc) -> R,
) -> Option<R> {
    let provider = router.state.active_provider();
    let any = provider.as_any()?;
    let torrent = any.downcast_ref::<TorrentFileSystemRpc>()?;
    Some(f(torrent))
}

fn torrent_path(router: &panel_router::PanelRouter) -> Option<String> {
    with_torrent(router, |t| t.torrent_path().to_string())
}

impl TorrentToolbar {
    pub fn refresh(&self, router: &panel_router::PanelRouter) {
        let path = torrent_path(router);
        let Some(path) = path else {
            self.container.set_visible(false);
            return;
        };
        self.container.set_visible(true);
        let running = virtualfs::torrent_session::is_running(&path);
        self.start_btn.set_sensitive(!running);
        self.stop_btn.set_sensitive(running);
        self.downloaded_btn.set_sensitive(running);
        self.peers_btn.set_sensitive(running);
    }
}

pub fn build(router: Rc<panel_router::PanelRouter>) -> TorrentToolbar {
    let container = gtk::Box::builder()
        .orientation(gtk::Orientation::Horizontal)
        .spacing(6)
        .margin_top(4)
        .margin_bottom(4)
        .margin_start(6)
        .margin_end(6)
        .visible(false)
        .build();

    let all_btn = gtk::ToggleButton::builder()
        .label(&*crate::i18n::tr("torrent.view_all"))
        .active(true)
        .build();
    let downloaded_btn = gtk::ToggleButton::builder()
        .label(&*crate::i18n::tr("torrent.view_downloaded"))
        .group(&all_btn)
        .build();
    let peers_btn = gtk::ToggleButton::builder()
        .label(&*crate::i18n::tr("torrent.view_peers"))
        .group(&all_btn)
        .build();
    let start_btn = gtk::Button::builder()
        .label(&*crate::i18n::tr("torrent.start"))
        .build();
    let stop_btn = gtk::Button::builder()
        .label(&*crate::i18n::tr("torrent.stop"))
        .build();
    let cleanup_btn = gtk::Button::builder()
        .label(&*crate::i18n::tr("torrent.cleanup"))
        .build();

    for (btn, view) in [
        (&all_btn, TorrentView::All),
        (&downloaded_btn, TorrentView::Downloaded),
        (&peers_btn, TorrentView::Peers),
    ] {
        let router = router.clone();
        btn.connect_toggled(move |b| {
            if !b.is_active() {
                return;
            }
            with_torrent(&router, |t| t.set_view(view));
            router.refresh_spawned();
        });
    }

    {
        let router = router.clone();
        start_btn.connect_clicked(move |_| {
            let Some(path) = torrent_path(&router) else {
                return;
            };
            let router = router.clone();
            gtk::glib::spawn_future_local(async move {
                let bytes = match std::fs::read(&path) {
                    Ok(b) => b,
                    Err(_) => return,
                };
                let _ = virtualfs::torrent_session::start(&path, bytes, None).await;
                router.refresh_spawned();
            });
        });
    }
    {
        let router = router.clone();
        stop_btn.connect_clicked(move |_| {
            let Some(path) = torrent_path(&router) else {
                return;
            };
            let router = router.clone();
            gtk::glib::spawn_future_local(async move {
                let _ = virtualfs::torrent_session::stop(&path).await;
                router.refresh_spawned();
            });
        });
    }
    {
        let router = router.clone();
        cleanup_btn.connect_clicked(move |btn| {
            let Some(path) = torrent_path(&router) else {
                return;
            };
            let Some(window) = btn.root().and_downcast::<gtk::Window>() else {
                return;
            };
            let dialog = adw::AlertDialog::builder()
                .heading(&*crate::i18n::tr("torrent.cleanup_heading"))
                .body(&*crate::i18n::tr("torrent.cleanup_body"))
                .build();
            let check = gtk::CheckButton::builder()
                .label(&*crate::i18n::tr("torrent.cleanup_delete_file"))
                .build();
            dialog.set_extra_child(Some(&check));
            dialog.add_response("cancel", &crate::i18n::tr("common.cancel"));
            dialog.add_response("wipe", &crate::i18n::tr("torrent.cleanup_confirm"));
            dialog.set_response_appearance("wipe", adw::ResponseAppearance::Destructive);
            let router = router.clone();
            dialog.connect_response(None, move |d, response| {
                if response != "wipe" {
                    return;
                }
                let also_torrent = check.is_active();
                let path = path.clone();
                let router = router.clone();
                d.close();
                gtk::glib::spawn_future_local(async move {
                    let _ = virtualfs::torrent_session::cleanup(&path, also_torrent).await;
                    router.refresh_spawned();
                });
            });
            dialog.present(Some(&window));
        });
    }

    container.append(&all_btn);
    container.append(&downloaded_btn);
    container.append(&peers_btn);
    container.append(&gtk::Separator::new(gtk::Orientation::Vertical));
    container.append(&start_btn);
    container.append(&stop_btn);
    container.append(&gtk::Separator::new(gtk::Orientation::Vertical));
    container.append(&cleanup_btn);

    TorrentToolbar {
        container,
        all_btn,
        downloaded_btn,
        peers_btn,
        start_btn,
        stop_btn,
    }
}
