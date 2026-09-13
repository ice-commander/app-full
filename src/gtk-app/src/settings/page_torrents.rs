use adw::prelude::*;
use gtk::{Align, Box, Label};
use std::rc::Rc;

pub fn seeding(config: &client_config::AppConfig) -> bool {
    config
        .get::<bool>(virtualfs::torrent_session::SEEDING_KEY)
        .unwrap_or(true)
}

pub(super) fn build(
    page_box: &Box,
    config: client_config::AppConfig,
    on_changed: Rc<dyn Fn() + 'static>,
) {
    let title = Label::builder()
        .label(&format!(
            "<span size='x-large' weight='bold'>{}</span>",
            crate::i18n::tr("settings.cat_torrents")
        ))
        .use_markup(true)
        .halign(Align::Start)
        .margin_bottom(16)
        .build();
    page_box.append(&title);

    let group = adw::PreferencesGroup::builder()
        .title(&*crate::i18n::tr("settings.torrent_group"))
        .description(&*crate::i18n::tr("settings.desc_torrent_group"))
        .build();

    let row = adw::SwitchRow::builder()
        .title(&*crate::i18n::tr("settings.torrent_seed"))
        .subtitle(&*crate::i18n::tr("settings.desc_torrent_seed"))
        .active(seeding(&config))
        .build();
    {
        let config = config.clone();
        let on_changed = on_changed.clone();
        row.connect_active_notify(move |r| {
            config.set(virtualfs::torrent_session::SEEDING_KEY, r.is_active());
            virtualfs::torrent_session::set_seeding_enabled(r.is_active());
            on_changed();
        });
    }
    group.add(&row);
    page_box.append(&group);
}
