import socket, time

for host, port in [("192.168.0.50", 59425), ("host.docker.internal", 59425)]:
    t0 = time.monotonic()
    try:
        s = socket.create_connection((host, port), timeout=5)
        elapsed = (time.monotonic() - t0) * 1000
        print(f"{host}:{port} -> OK w {elapsed:.1f} ms")
        s.close()
    except Exception as exc:
        elapsed = (time.monotonic() - t0) * 1000
        print(f"{host}:{port} -> BLAD po {elapsed:.1f} ms: {type(exc).__name__}: {exc}")
