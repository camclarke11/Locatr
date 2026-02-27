import { useMemo } from "react";
import DeckGL from "@deck.gl/react";
import { TripsLayer } from "@deck.gl/geo-layers";
import Map from "react-map-gl/maplibre";

import { LONDON_VIEW_STATE, MAP_STYLE_URL } from "./config";
import { PlaybackControls } from "./components/PlaybackControls";
import { useTripPlayback } from "./hooks/useTripPlayback";
import type { DecodedTrip } from "./types";

import "./App.css";

function App() {
  const {
    status,
    error,
    jumpError,
    bounds,
    currentTimeMs,
    isPlaying,
    speed,
    decodedTrips,
    setPlaybackTime,
    togglePlay,
    setSpeed,
    jumpToNaturalLanguage,
  } = useTripPlayback();

  const layers = useMemo(
    () => [
      new TripsLayer<DecodedTrip>({
        id: "santander-trips",
        data: decodedTrips,
        getPath: (trip) => trip.path,
        getTimestamps: (trip) => trip.timestamps,
        getColor: [12, 144, 255],
        opacity: 0.85,
        widthMinPixels: 2,
        rounded: true,
        trailLength: 8 * 60 * 1000,
        currentTime: currentTimeMs,
        capRounded: true,
        jointRounded: true,
      }),
    ],
    [currentTimeMs, decodedTrips],
  );

  if (status === "loading" || !bounds) {
    return (
      <main className="appStatus">
        <h1>London Bike Trips</h1>
        <p>Loading DuckDB WASM and trip metadata...</p>
      </main>
    );
  }

  if (status === "error") {
    return (
      <main className="appStatus">
        <h1>London Bike Trips</h1>
        <p>{error ?? "Unexpected error while loading data."}</p>
      </main>
    );
  }

  return (
    <main className="appRoot">
      <DeckGL
        initialViewState={LONDON_VIEW_STATE}
        controller
        layers={layers}
        style={{
          position: "absolute",
          top: "0",
          right: "0",
          bottom: "0",
          left: "0",
        }}
      >
        <Map reuseMaps mapStyle={MAP_STYLE_URL} />
      </DeckGL>

      <header className="topBanner">
        <h1>London Santander Trips (DuckDB WASM + TripsLayer)</h1>
        <p>{decodedTrips.length.toLocaleString()} decoded trips in active buffer</p>
      </header>

      <PlaybackControls
        minTimeMs={bounds.minMs}
        maxTimeMs={bounds.maxMs}
        currentTimeMs={currentTimeMs}
        isPlaying={isPlaying}
        speed={speed}
        onSetTime={setPlaybackTime}
        onTogglePlay={togglePlay}
        onSetSpeed={(nextSpeed) => setSpeed(nextSpeed)}
        onNaturalLanguageJump={jumpToNaturalLanguage}
        jumpError={jumpError}
      />
    </main>
  );
}

export default App;
