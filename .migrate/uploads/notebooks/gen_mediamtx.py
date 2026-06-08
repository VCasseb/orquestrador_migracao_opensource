#!/usr/bin/env python3
"""Gera config/mediamtx.yml a partir de config/cameras.yaml.

Mantém o cameras.yaml como fonte única de verdade: adicionar/remover uma
câmera é só editar o cameras.yaml e rodar este script (o docker-compose já
faz isso automaticamente via serviço 'config-gen').
"""
from __future__ import annotations

import pathlib
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
CAMERAS_FILE = ROOT / "config" / "cameras.yaml"
OUTPUT_FILE = ROOT / "config" / "mediamtx.yml"

# Globais do MediaMTX. webrtcAdditionalHosts é injetado via variável de
# ambiente MTX_WEBRTCADDITIONALHOSTS no docker-compose (= IP do PC na LAN).
BASE_CONFIG = {
    "logLevel": "info",
    "api": True,
    "apiAddress": ":9997",
    # Só precisamos de WebRTC (browser) e RTSP (worker de IA lê daqui).
    "rtsp": True,
    "rtmp": False,
    "hls": False,
    "webrtc": True,
    "webrtcAddress": ":8889",
    "webrtcLocalUDPAddress": ":8189",
    "webrtcLocalTCPAddress": ":8189",
    # Sem autenticação (uso local). Libera tudo, inclusive a API de debug
    # (9997) a partir de qualquer IP — por padrão o MediaMTX só liberaria
    # a API pra localhost interno.
    "authMethod": "internal",
    "authInternalUsers": [
        {
            "user": "any",
            "permissions": [
                {"action": "publish"},
                {"action": "read"},
                {"action": "playback"},
                {"action": "api"},
                {"action": "metrics"},
                {"action": "pprof"},
            ],
        }
    ],
}


def build_paths(cameras: list[dict]) -> dict:
    paths: dict[str, dict] = {}
    for cam in cameras:
        cam_id = str(cam["id"]).strip()
        rtsp = str(cam["rtsp"]).strip()
        if not cam_id or not rtsp:
            print(f"[gen_mediamtx] ignorando câmera inválida: {cam!r}", file=sys.stderr)
            continue
        paths[cam_id] = {
            "source": rtsp,
            # Mantém a conexão RTSP sempre aberta: assim que o navegador abre,
            # o stream já está fluindo (sem esperar pull + 1º keyframe, que
            # custava 3–8s). Custo: 1 conexão persistente por câmera.
            "sourceOnDemand": False,
        }
    return paths


def main() -> int:
    if not CAMERAS_FILE.exists():
        print(f"[gen_mediamtx] não encontrei {CAMERAS_FILE}", file=sys.stderr)
        return 1

    with CAMERAS_FILE.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}

    cameras = data.get("cameras") or []
    if not cameras:
        print("[gen_mediamtx] nenhuma câmera definida em cameras.yaml", file=sys.stderr)
        return 1

    config = dict(BASE_CONFIG)
    config["paths"] = build_paths(cameras)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    header = (
        "# ARQUIVO GERADO AUTOMATICAMENTE por scripts/gen_mediamtx.py\n"
        "# NÃO edite à mão — altere config/cameras.yaml e rode de novo.\n"
    )
    with OUTPUT_FILE.open("w", encoding="utf-8") as fh:
        fh.write(header)
        yaml.safe_dump(config, fh, sort_keys=False, allow_unicode=True)

    print(f"[gen_mediamtx] {OUTPUT_FILE} gerado com {len(config['paths'])} câmera(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
