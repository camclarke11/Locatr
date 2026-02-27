import { useState, type FormEvent } from "react";

import { formatLondonTime } from "../lib/time";
import type { PlaybackSpeed } from "../types";

type PlaybackControlsProps = {
  minTimeMs: number;
  maxTimeMs: number;
  currentTimeMs: number;
  isPlaying: boolean;
  speed: PlaybackSpeed;
  onSetTime: (value: number) => void;
  onTogglePlay: () => void;
  onSetSpeed: (speed: PlaybackSpeed) => void;
  onNaturalLanguageJump: (query: string) => void;
  jumpError: string | null;
};

const SPEED_OPTIONS: PlaybackSpeed[] = [1, 10, 60];

export function PlaybackControls({
  minTimeMs,
  maxTimeMs,
  currentTimeMs,
  isPlaying,
  speed,
  onSetTime,
  onTogglePlay,
  onSetSpeed,
  onNaturalLanguageJump,
  jumpError,
}: PlaybackControlsProps) {
  const [query, setQuery] = useState("");

  const handleSubmit = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault();
    if (!query.trim()) {
      return;
    }
    onNaturalLanguageJump(query.trim());
  };

  return (
    <section className="controls">
      <div className="controls__row">
        <button type="button" onClick={onTogglePlay} className="controls__button">
          {isPlaying ? "Pause" : "Play"}
        </button>
        <label className="controls__label">
          Speed
          <select
            className="controls__select"
            value={speed}
            onChange={(event) => onSetSpeed(Number(event.target.value) as PlaybackSpeed)}
          >
            {SPEED_OPTIONS.map((option) => (
              <option key={option} value={option}>
                {option}x
              </option>
            ))}
          </select>
        </label>
        <span className="controls__time">{formatLondonTime(currentTimeMs)}</span>
      </div>

      <input
        type="range"
        className="controls__slider"
        min={minTimeMs}
        max={maxTimeMs}
        step={60_000}
        value={currentTimeMs}
        onChange={(event) => onSetTime(Number(event.target.value))}
      />

      <form className="controls__search" onSubmit={handleSubmit}>
        <input
          className="controls__searchInput"
          placeholder='Jump to: "Last Friday at 5pm"'
          value={query}
          onChange={(event) => setQuery(event.target.value)}
        />
        <button className="controls__button" type="submit">
          Jump
        </button>
      </form>
      {jumpError && <p className="controls__error">{jumpError}</p>}
      <p className="controls__hint">
        Keyboard: Space (play/pause), Left/Right (±5 min), Shift+Left/Right (±30 min)
      </p>
    </section>
  );
}
