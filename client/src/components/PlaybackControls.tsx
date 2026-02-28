import { useState, type FormEvent } from "react";

import { formatLondonTime } from "../lib/time";
import type { PlaybackSpeed } from "../types";

type PlaybackControlsProps = {
  minTimeMs: number;
  maxTimeMs: number;
  currentTimeMs: number;
  isPlaying: boolean;
  isRandomRideFollowEnabled: boolean;
  speed: PlaybackSpeed;
  onSetTime: (value: number) => void;
  onTogglePlay: () => void;
  onRandomRideFollow: () => void;
  onRandomTimeJump: () => void;
  onSetSpeed: (speed: PlaybackSpeed) => void;
  onNaturalLanguageJump: (query: string) => void;
  jumpError: string | null;
};

const SPEED_OPTIONS: PlaybackSpeed[] = [1, 10, 60, 100];

export function PlaybackControls({
  minTimeMs,
  maxTimeMs,
  currentTimeMs,
  isPlaying,
  isRandomRideFollowEnabled,
  speed,
  onSetTime,
  onTogglePlay,
  onRandomRideFollow,
  onRandomTimeJump,
  onSetSpeed,
  onNaturalLanguageJump,
  jumpError,
}: PlaybackControlsProps) {
  const [query, setQuery] = useState("");
  const [showAbout, setShowAbout] = useState(false);

  const handleSubmit = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault();
    if (!query.trim()) {
      return;
    }
    onNaturalLanguageJump(query.trim());
  };

  const minLabel = new Date(minTimeMs).toLocaleDateString("en-GB", {
    month: "short",
    day: "numeric",
  });
  const maxLabel = new Date(maxTimeMs).toLocaleDateString("en-GB", {
    month: "short",
    day: "numeric",
  });

  return (
    <section className="commandRail">
      <form className="commandRail__row commandRail__row--search" onSubmit={handleSubmit}>
        <span className="commandRail__icon" aria-hidden="true">
          S
        </span>
        <input
          className="commandRail__searchInput"
          placeholder='Search time phrase...'
          value={query}
          onChange={(event) => setQuery(event.target.value)}
          aria-label="Search timeline phrase"
        />
        <kbd className="commandRail__key">K</kbd>
      </form>

      <button type="button" onClick={onTogglePlay} className="commandRail__row">
        <span className="commandRail__icon" aria-hidden="true">
          {isPlaying ? "||" : ">"}
        </span>
        <span>{isPlaying ? "Pause" : "Play"}</span>
        <kbd className="commandRail__key">Space</kbd>
      </button>

      <button
        type="button"
        onClick={onRandomRideFollow}
        className={`commandRail__row ${isRandomRideFollowEnabled ? "isActive" : ""}`}
      >
        <span className="commandRail__icon" aria-hidden="true">
          R
        </span>
        <span>{isRandomRideFollowEnabled ? "Following Random" : "Random Ride"}</span>
        <kbd className="commandRail__key">R</kbd>
      </button>

      <label className="commandRail__row commandRail__row--speed">
        <span className="commandRail__icon" aria-hidden="true">
          T
        </span>
        <span>Speed</span>
        <select
          className="commandRail__select"
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

      <button
        type="button"
        className="commandRail__row"
        onClick={() => setShowAbout((current) => !current)}
      >
        <span className="commandRail__icon" aria-hidden="true">
          i
        </span>
        <span>About</span>
        <kbd className="commandRail__key">A</kbd>
      </button>

      {showAbout ? (
        <p className="commandRail__about">
          London Santander playback on DuckDB-WASM. Space toggles playback, arrows step time.
        </p>
      ) : null}

      <div className="commandRail__timeline">
        <div className="commandRail__timeRow">
          <p className="commandRail__time">{formatLondonTime(currentTimeMs)}</p>
          <button
            type="button"
            className="commandRail__timeRandom"
            onClick={onRandomTimeJump}
          >
            Random time
          </button>
        </div>
        <input
          type="range"
          className="commandRail__slider"
          min={minTimeMs}
          max={maxTimeMs}
          step={60_000}
          value={currentTimeMs}
          onChange={(event) => onSetTime(Number(event.target.value))}
          aria-label="Playback timeline"
        />
        <div className="commandRail__bounds">
          <span>{minLabel}</span>
          <span>{maxLabel}</span>
        </div>
      </div>

      {jumpError ? <p className="commandRail__error">{jumpError}</p> : null}
    </section>
  );
}
