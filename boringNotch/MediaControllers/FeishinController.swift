//
//  FeishinController.swift
//  boringNotch
//
//  Feishin (Navidrome/Jellyfin desktop client) through its remote-control WebSocket.
//  Enable it in Feishin → Settings → Window → Remote control, then store "user:password":
//  defaults write theboringteam.boringnotch feishinRemoteAuth "user:password"
//

import Foundation
import Combine
import Defaults

class FeishinController: MediaControllerProtocol {
    static let bundleIdentifier = "org.jeffvli.feishin"
    private let url = URL(string: "ws://127.0.0.1:4333")!

    @Published private var playbackState = PlaybackState(bundleIdentifier: FeishinController.bundleIdentifier)

    var playbackStatePublisher: AnyPublisher<PlaybackState, Never> {
        $playbackState.eraseToAnyPublisher()
    }

    var supportsVolumeControl: Bool { true }
    var supportsFavorite: Bool { true }

    private var socket: URLSessionWebSocketTask?
    private var songID: String?
    private var imageURL: String?

    init() {
        connect()
    }

    deinit {
        socket?.cancel()
    }

    // MARK: - Connection

    private func connect() {
        let ws = URLSession.shared.webSocketTask(with: url)
        socket = ws
        ws.resume()
        if let auth = Defaults[.feishinRemoteAuth], !auth.isEmpty {
            let header = "Basic " + Data(auth.utf8).base64EncodedString()
            Task { await send(["event": "authenticate", "header": header]) }
        }
        receive(ws)
    }

    private func receive(_ ws: URLSessionWebSocketTask) {
        ws.receive { [weak self] result in
            guard let self, ws === self.socket else { return }
            switch result {
            case .success(.string(let text)):
                DispatchQueue.main.async { self.handle(text) }
                self.receive(ws)
            case .success:
                self.receive(ws)
            case .failure:
                // Feishin closed or not running: clear the player and retry.
                DispatchQueue.main.async { self.apply(song: nil, to: &self.playbackState) }
                DispatchQueue.main.asyncAfter(deadline: .now() + 3) { [weak self] in self?.connect() }
            }
        }
    }

    private func send(_ payload: [String: Any]) async {
        guard let socket,
              let data = try? JSONSerialization.data(withJSONObject: payload),
              let text = String(data: data, encoding: .utf8) else { return }
        try? await socket.send(.string(text))
    }

    // MARK: - Server events

    // Each event is applied to a copy and published once, so a new song is one state change.
    private func handle(_ text: String) {
        guard let msg = try? JSONSerialization.jsonObject(with: Data(text.utf8)) as? [String: Any],
              let event = msg["event"] as? String else { return }
        let data = msg["data"]
        var state = playbackState
        switch event {
        case "state":
            guard let s = data as? [String: Any] else { return }
            apply(song: s["song"] as? [String: Any], to: &state)
            state.isPlaying = s["status"] as? String == "playing"
            state.currentTime = s["position"] as? Double ?? 0
            state.volume = (s["volume"] as? Double ?? 100) / 100
            state.isShuffled = s["shuffle"] as? Bool ?? false
            state.repeatMode = repeatMode(s["repeat"])
        case "song":
            apply(song: data as? [String: Any], to: &state)
        case "playback":
            state.isPlaying = data as? String == "playing"
        case "position":
            guard let position = data as? Double else { return }
            state.currentTime = position
        case "volume":
            guard let volume = data as? Double else { return }
            state.volume = volume / 100
        case "shuffle":
            state.isShuffled = data as? Bool ?? false
        case "repeat":
            state.repeatMode = repeatMode(data)
        case "favorite":
            guard let f = data as? [String: Any], f["id"] as? String == songID else { return }
            state.isFavorite = f["favorite"] as? Bool ?? false
        default:
            return
        }
        state.lastUpdated = Date()
        playbackState = state
    }

    private func apply(song: [String: Any]?, to state: inout PlaybackState) {
        guard let song, let id = song["id"] as? String else {
            songID = nil
            imageURL = nil
            state = PlaybackState(bundleIdentifier: Self.bundleIdentifier, title: "", artist: "", album: "")
            return
        }
        guard id != songID else { return }
        songID = id
        state.title = song["name"] as? String ?? ""
        state.artist = song["artistName"] as? String ?? ""
        state.album = song["album"] as? String ?? ""
        state.duration = (song["duration"] as? Double ?? 0) / 1000
        state.isFavorite = song["userFavorite"] as? Bool ?? false
        state.artwork = nil
        imageURL = song["imageUrl"] as? String
        if let imageURL, let url = URL(string: imageURL) {
            Task { @MainActor in
                guard let data = try? await ImageService.shared.fetchImageData(from: url), self.songID == id else { return }
                self.playbackState.artwork = data
            }
        }
    }

    private func repeatMode(_ value: Any?) -> RepeatMode {
        switch value as? String {
        case "all": return .all
        case "one": return .one
        default: return .off
        }
    }

    // MARK: - Lyrics

    /// Synced lyrics from Navidrome (the .lrc next to each track), reusing the auth params
    /// Feishin already puts in the cover-art URL. Empty when the song has none.
    func syncedLyrics() async -> [(time: Double, text: String)] {
        guard let songID, let imageURL, var url = URLComponents(string: imageURL) else { return [] }
        url.path = url.path.replacingOccurrences(of: "getCoverArt", with: "getLyricsBySongId")
        url.queryItems = (url.queryItems ?? []).filter { !["id", "size", "f"].contains($0.name) }
            + [URLQueryItem(name: "id", value: songID), URLQueryItem(name: "f", value: "json")]
        guard let request = url.url,
              let response = try? await URLSession.shared.data(from: request),
              let json = try? JSONSerialization.jsonObject(with: response.0) as? [String: Any],
              let body = json["subsonic-response"] as? [String: Any],
              let list = (body["lyricsList"] as? [String: Any])?["structuredLyrics"] as? [[String: Any]],
              let lyrics = list.first(where: { $0["synced"] as? Bool == true }) else { return [] }
        // OpenSubsonic offset is in ms; positive means lyrics show sooner.
        let offset = (lyrics["offset"] as? Double ?? 0) / 1000
        return (lyrics["line"] as? [[String: Any]] ?? []).compactMap { line in
            guard let start = line["start"] as? Double else { return nil }
            let text = (line["value"] as? String ?? "").trimmingCharacters(in: .whitespaces)
            return (start / 1000 - offset, text.isEmpty ? "♪" : text)
        }
    }

    // MARK: - Protocol Implementation

    // Feishin's remote falls through from "play" into "previous" (missing break upstream);
    // its own web remote sends the same event, so this matches Feishin's behavior.
    func play() async { await send(["event": "play"]) }
    func pause() async { await send(["event": "pause"]) }
    func togglePlay() async {
        if playbackState.isPlaying { await pause() } else { await play() }
    }
    func nextTrack() async { await send(["event": "next"]) }
    func previousTrack() async { await send(["event": "previous"]) }
    func seek(to time: Double) async { await send(["event": "position", "position": time]) }
    func toggleShuffle() async { await send(["event": "shuffle"]) }
    func toggleRepeat() async { await send(["event": "repeat"]) }
    func setVolume(_ level: Double) async { await send(["event": "volume", "volume": Int(level * 100)]) }

    func setFavorite(_ favorite: Bool) async {
        guard let songID else { return }
        await send(["event": "favorite", "favorite": favorite, "id": songID])
    }

    func isActive() -> Bool {
        socket?.state == .running && songID != nil
    }

    func updatePlaybackInfo() async {
        // Feishin pushes every change over the socket; nothing to poll.
    }
}
