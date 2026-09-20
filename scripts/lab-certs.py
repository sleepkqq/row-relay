"""Generate disposable loopback Kafka TLS fixtures, never deployment keys."""

from pathlib import Path
import subprocess
import tempfile

directory = Path(__file__).resolve().parents[1] / "local" / "secrets"
directory.mkdir(mode=0o700, exist_ok=True)
certificate, bundle = directory / "ca.pem", directory / "broker.pem"
if certificate.exists() and bundle.exists():
    raise SystemExit(0)
if certificate.exists() or bundle.exists():
    raise SystemExit("Incomplete lab TLS fixture: inspect local/secrets before recreating it")

with tempfile.TemporaryDirectory(dir=directory) as temporary:
    key, cert = Path(temporary) / "key.pem", Path(temporary) / "cert.pem"
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
        "-sha256", "-days", "30", "-subj", "/CN=rowrelay-local-lab",
        "-addext", "subjectAltName=DNS:localhost,DNS:kafka,IP:127.0.0.1",
        "-keyout", str(key), "-out", str(cert),
    ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    certificate.write_bytes(cert.read_bytes())
    bundle.write_bytes(key.read_bytes() + cert.read_bytes())
    # The private host directory protects the fixture; the file bind mount must
    # also be readable by the image's UID 1000 on hosts with a different UID.
    bundle.chmod(0o644)
print("Generated disposable Kafka TLS fixture in local/secrets")
