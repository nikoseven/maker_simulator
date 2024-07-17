use std::ops::RangeInclusive;

use eframe::egui::{self, Color32, Frame, Margin, RichText, Widget};
use egui_plot::{
    BoxElem, BoxPlot, BoxSpread, GridMark, Legend, Line, Plot, PlotPoints, PlotUi, Points,
};
use time::OffsetDateTime;

use crate::{
    candle::OhlcvCandle,
    vis_data::{
        compute_candles_from_market_trades, DataState, MakerOrderBrief, TimeInMs, TradeBrief,
    },
};

type UpdateFnType = dyn FnMut(&mut DataState) -> bool;
pub struct VisApp {
    update_data_fn: Option<Box<UpdateFnType>>,
    state: DataState,
    ui_state: VisAppUiState,
}

struct VisAppUiState {
    candle_period_ms: TimeInMs,
    show_account_trade: bool,
    show_order_brief: bool,
}

impl VisAppUiState {
    const CANDLE_PERIODS: [(&'static str, TimeInMs); 8] = [
        ("1m", 60 * 1000),
        ("5m", 5 * 60 * 1000),
        ("15m", 15 * 60 * 1000),
        ("30m", 30 * 60 * 1000),
        ("1h", 60 * 60 * 1000),
        ("4h", 4 * 60 * 60 * 1000),
        ("8h", 8 * 60 * 60 * 1000),
        ("1d", 24 * 60 * 60 * 1000),
    ];

    fn candle_period_str(t: TimeInMs) -> &'static str {
        Self::CANDLE_PERIODS
            .iter()
            .find_map(|(s, p)| if *p == t { Some(*s) } else { None })
            .unwrap_or("custom")
    }
}

impl VisApp {
    pub fn with_update_data_fn(
        mut self,
        update_fn: Box<dyn FnMut(&mut DataState) -> bool>,
    ) -> Self {
        self.update_data_fn = update_fn.into();
        self
    }
}

impl Default for VisApp {
    fn default() -> Self {
        Self {
            update_data_fn: None,
            state: DataState::default(),
            ui_state: VisAppUiState {
                candle_period_ms: 15 * 60 * 1000,
                show_account_trade: false,
                show_order_brief: false,
            },
        }
    }
}

impl eframe::App for VisApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if let Some(f) = self.update_data_fn.as_mut() {
            if f(&mut self.state) {
                ctx.request_repaint();
            }
        }
        egui::TopBottomPanel::bottom("account_view")
            .default_height(200.0)
            .resizable(true)
            .frame(Frame {
                inner_margin: Margin::symmetric(0.0, 0.0),
                ..Default::default()
            })
            .show(ctx, |ui| {
                let layout = egui::Layout::top_down(egui::Align::Min)
                    .with_cross_justify(true)
                    .with_main_align(egui::Align::TOP);
                ui.with_layout(layout, |ui| self.account_view(ui));
            });
        egui::CentralPanel::default()
            .frame(Frame {
                inner_margin: Margin::symmetric(0.0, 0.0),
                ..Default::default()
            })
            .show(ctx, |ui| {
                let layout = egui::Layout::top_down(egui::Align::Min).with_cross_justify(true);
                ui.with_layout(layout, |ui| self.market_view(ui));
            });
    }
}

impl VisApp {
    fn account_view(&mut self, ui: &mut egui::Ui) {
        ui.heading("Account view");

        let plot = Plot::new("account_plot")
            .x_axis_formatter(timestamp_axis_formatter)
            .show_axes([true, true])
            .show_grid([true, true])
            .legend(Legend::default())
            .link_axis("timeline_linkgroup", true, false)
            .link_cursor("timeline_linkgroup", true, false);
        plot.show(ui, |plot_ui| {
            self.state
                .account_asset_history
                .iter()
                .for_each(|(asset, history)| {
                    let plot_points = history
                        .iter()
                        .map(|(ts_ms, balance)| [*ts_ms as f64 / 1000.0, *balance])
                        .collect::<Vec<_>>();
                    plot_ui.line(Line::new(plot_points).name(asset));
                });
        });
    }

    fn market_view(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            egui::Label::new(RichText::from("Market view").heading())
                .selectable(false)
                .ui(ui);

            egui::ComboBox::from_id_source("candle_period")
                .selected_text(VisAppUiState::candle_period_str(
                    self.ui_state.candle_period_ms,
                ))
                .show_ui(ui, |ui| {
                    for v in &VisAppUiState::CANDLE_PERIODS {
                        let (text, selected_value) = v;
                        ui.selectable_value(
                            &mut self.ui_state.candle_period_ms,
                            *selected_value,
                            *text,
                        );
                    }
                });
            ui.checkbox(&mut self.ui_state.show_account_trade, "TradeMarker");
            ui.checkbox(&mut self.ui_state.show_order_brief, "OrderBrief");
        });
        let plot = Plot::new("market_plot")
            .x_axis_formatter(timestamp_axis_formatter)
            .show_axes([true, true])
            .show_grid([true, true])
            .link_axis("timeline_linkgroup", true, false)
            .link_cursor("timeline_linkgroup", true, false);
        plot.show(ui, |plot_ui| {
            // draw candles
            let period_ms = self.ui_state.candle_period_ms;
            let candles = compute_candles_from_market_trades(
                &self.state.market_trades,
                self.state.market_trades.first().map_or(0, |f| f.time),
                period_ms,
            );
            Self::draw_candle(plot_ui, candles, period_ms);
            // draw trades
            if self.ui_state.show_account_trade {
                Self::draw_account_trades(plot_ui, &self.state.account_trades);
            }
            // draw orders
            if self.ui_state.show_order_brief {
                Self::draw_order_briefs(plot_ui, self.state.order_briefs.values());
            }
        });
    }

    fn draw_candle(
        plot_ui: &mut PlotUi,
        candles: impl Iterator<Item = (TimeInMs, OhlcvCandle)>,
        period_ms: TimeInMs,
    ) {
        let make_box_elem = |(time_ms, candle): (TimeInMs, OhlcvCandle)| {
            BoxElem::new(
                time_ms as f64 / 1000.0 + 0.5 * period_ms as f64 / 1000.0,
                BoxSpread::new(
                    candle.low,
                    candle.open,
                    candle.close,
                    candle.close,
                    candle.high,
                ),
            )
            .box_width(period_ms as f64 / 1000.0)
        };
        let mut incr_boxes = vec![];
        let mut decr_boxes = vec![];
        candles.for_each(|(time_ms, candle)| {
            let is_incr = candle.open < candle.close;
            let box_elem = make_box_elem((time_ms, candle));
            if is_incr {
                incr_boxes.push(box_elem);
            } else {
                decr_boxes.push(box_elem);
            }
        });
        const RED: Color32 = Color32::from_rgb(200, 0, 0);
        const GREEN: Color32 = Color32::from_rgb(0, 255, 0);
        plot_ui.box_plot(BoxPlot::new(incr_boxes).color(RED));
        plot_ui.box_plot(BoxPlot::new(decr_boxes).color(GREEN));
    }

    fn draw_account_trades(plot_ui: &mut PlotUi, account_trades: &[TradeBrief]) {
        let mut buy_points = vec![];
        let mut sell_points = vec![];
        account_trades.iter().for_each(|trade_brief| {
            let point = [trade_brief.time as f64 / 1000.0, trade_brief.price];
            if trade_brief.is_buy {
                buy_points.push(point);
            } else {
                sell_points.push(point);
            }
        });
        let buy_points = Points::new(buy_points)
            .name("buy")
            .color(Color32::from_rgb(255, 0, 0))
            .filled(false)
            .shape(egui_plot::MarkerShape::Up)
            .radius(5.0);
        let sell_points = Points::new(sell_points)
            .name("sell")
            .color(Color32::from_rgb(0, 255, 0))
            .filled(false)
            .shape(egui_plot::MarkerShape::Down)
            .radius(5.0);
        plot_ui.points(buy_points);
        plot_ui.points(sell_points);
    }

    fn draw_order_briefs<'a>(
        plot_ui: &mut PlotUi,
        briefs: impl Iterator<Item = &'a MakerOrderBrief>,
    ) {
        const BUY_ORDER_COLOR: Color32 = Color32::from_rgb(255, 100, 0);
        const SELL_ORDER_COLOR: Color32 = Color32::from_rgb(100, 255, 0);

        briefs
            .filter(|brief| brief.created_at > 0 && brief.ended_at > 0)
            .for_each(|brief| {
                let l = Line::new(Into::<PlotPoints>::into(PlotPoints::new(
                    [
                        [brief.created_at as f64 / 1000.0, brief.price],
                        [brief.ended_at as f64 / 1000.0, brief.price],
                    ]
                    .into(),
                )))
                .width(3.0)
                .color(if brief.is_buy {
                    BUY_ORDER_COLOR
                } else {
                    SELL_ORDER_COLOR
                });
                plot_ui.line(l);
            });
    }
}

fn timestamp_axis_formatter(
    mark: GridMark,
    _max_digits: usize,
    _range: &RangeInclusive<f64>,
) -> String {
    let duration_since_epoch = mark.value;
    convert_timestamp_to_string(duration_since_epoch)
}

fn convert_timestamp_to_string(duration_since_epoch: f64) -> String {
    if duration_since_epoch < 0.0 {
        return "".to_string();
    }
    let dt: OffsetDateTime = std::time::SystemTime::UNIX_EPOCH
        .checked_add(std::time::Duration::from_secs_f64(duration_since_epoch))
        .unwrap()
        .into();
    // format time like 2000-01-01 00:00:00.000
    format!(
        "{}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
        dt.year(),
        dt.month() as i32,
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second(),
        dt.millisecond()
    )
}
