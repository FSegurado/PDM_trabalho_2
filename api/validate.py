"""Confere se a API reproduz o ML.PREDICT do BigQuery na amostra de referência exportada.

Uso:  python api/validate.py https://URL-DA-API      (usa o token do gcloud)
"""
import glob, json, subprocess, sys, urllib.request

url = sys.argv[1].rstrip("/") + "/predict"
token = subprocess.run(["gcloud", "config", "config-helper", "--force-auth-refresh", "--format=value(credential.id_token)"], capture_output=True, text=True).stdout.strip()
subprocess.run(["gcloud", "storage", "cp", "gs://futdados/models/ballon_dor_btree/v1/preprocessing/reference_*.json",
                "/tmp/", "-q"], check=True)
refs = [json.loads(l) for p in glob.glob("/tmp/reference_*.json") for l in open(p) if l.strip()]
inst = [{k: v for k, v in r.items() if k not in ("player", "club_main", "predicted_ballon_dor_score")} for r in refs]
for i in inst:
    i["season"] = int(i["season"])
req = urllib.request.Request(url, data=json.dumps({"instances": inst}).encode(), method="POST",
                             headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
out = json.load(urllib.request.urlopen(req))["predicoes"]
diffs = [abs(o["predicted_ballon_dor_score"] - r["predicted_ballon_dor_score"]) for o, r in zip(out, refs)]
print(f"{len(refs)} jogadores | maior diferença API x ML.PREDICT = {max(diffs):.6f}")
for o, r in sorted(zip(out, refs), key=lambda x: -x[1]["predicted_ballon_dor_score"])[:5]:
    print(f"  {r['season']} {r['player']:<25} BQ={r['predicted_ballon_dor_score']:.4f}  API={o['predicted_ballon_dor_score']:.4f}")
sys.exit(0 if max(diffs) < 1e-3 else 1)
