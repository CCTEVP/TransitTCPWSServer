Place 1080×1920 clips here (v4):

- `video01.mp4` — idle / no tram (`RET_NO_TRAIN`)
- `video02.mp4` — arriving (`RET_TRAIN_ARRIVING_15S`); swap via `ARRIVING_VIDEO_SRC` in `app.js`
- `video03.mp4` — arrived (after countdown or `RET_TRAIN_ARRIVED`)
- `train-departed_1080x1920.mp4` — departed, then back to idle

`ARRIVING_MODE` in `app.js`: `"countdown"` (default, 10→0 overlay) or `"video"` (play clip through, no overlay).
