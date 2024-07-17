#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")] // hide console window on Windows in release

use eframe::egui;
use vis;

fn main() -> Result<(), eframe::Error> {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1200.0, 800.0]),
        default_theme: eframe::Theme::Dark,
        follow_system_theme: false,
        centered: true,
        ..Default::default()
    };
    eframe::run_native(
        "Stepper Visualization",
        options,
        Box::new(|cc| {
            // set scale to 1.25
            cc.egui_ctx.set_pixels_per_point(1.);
            // set font to mono font
            Box::<vis::vis_app::VisApp>::default()
        }),
    )
}
