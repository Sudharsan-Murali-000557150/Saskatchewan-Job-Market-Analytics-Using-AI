import ollama, json, sys

path = sys.argv[1] if len(sys.argv) > 1 else "posting.txt"
text = open(path, encoding="utf-8").read()

resp = ollama.chat(
    model="sk-jobs-qwen",
    messages=[{"role": "user", "content": text}],
    options={"temperature": 0},
)
raw = resp["message"]["content"]
try:
    print(json.dumps(json.loads(raw), indent=2, ensure_ascii=False))
except json.JSONDecodeError:
    print("Model did not return valid JSON:\n", raw)