import json
import sys

MODEL_ID_DEFAULT = "mlx-community/Qwen3.5-0.8B-OptiQ-4bit"
INSTALL_HINT = 'python3 -m pip install "mlx-lm>=0.30.7"'


def fail(message: str, code: int = 1) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(code)


try:
    from mlx_lm import load, generate
    from mlx_lm.sample_utils import make_sampler
except ImportError:
    fail(f'mlx-lm is not installed. Install it with: {INSTALL_HINT}', 2)


def normalize_text(value: str) -> str:
    return " ".join(str(value).split()).strip()


def build_prompt(tokenizer, law_dump: str) -> str:
    system_prompt = (
        "Tu raksti vienu īsu teikumu latviešu valodā par oficiālo normu tēmām. "
        "Raksti vienā teikumā, vienkāršā valodā un neitrāli. Atbildē dod tikai "
        "pašu kopsavilkumu, bez ievadfrāzēm. Nemin pantu numurus, likumu "
        "saīsinājumus, sodus, personas, vainu vai lietas faktus."
    )
    user_prompt = (
        f"Oficiālo normu tēmas:\n{law_dump}\n\n"
        "Uzraksti tieši vienu īsu kopsavilkuma teikumu."
    )

    if hasattr(tokenizer, "apply_chat_template"):
        try:
            return tokenizer.apply_chat_template(
                [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                tokenize=False,
                add_generation_prompt=True,
            )
        except Exception:
            pass

    return f"{system_prompt}\n\n{user_prompt}\n"


def clean_summary(text: str) -> str:
    normalized = normalize_text(text)
    if not normalized:
        return "Publiskajā rindā nav pietiekamu normu datu lietas rakstura kopsavilkumam."

    normalized = normalized.replace('"', "").replace("“", "").replace("”", "")
    normalized = normalized.removeprefix("Apsūdzībā minētās normas aptver tēmas: ").strip()
    normalized = normalized.removeprefix("Apsūdzībā minētās normas aptver tēmu: ").strip()
    normalized = normalized.removeprefix("Apsūdzībā minētās normas aptver: ").strip()
    normalized = normalized.removeprefix("Apsūdzībā minētās normas aptver ").strip()

    sentence_end = -1
    for index, char in enumerate(normalized):
        if char in ".!?":
            sentence_end = index
            break

    if sentence_end >= 0:
        normalized = normalized[: sentence_end + 1]
    else:
        normalized = normalized.rstrip(".") + "."

    return normalized


def main() -> None:
    payload = json.load(sys.stdin)
    items = payload.get("items", [])
    model_id = payload.get("model") or MODEL_ID_DEFAULT

    if not items:
      json.dump({"summaries": {}}, sys.stdout, ensure_ascii=False)
      return

    try:
        model, tokenizer = load(model_id)
    except Exception as exc:
        fail(f"Could not load MLX model '{model_id}': {exc}", 3)

    summaries = {}
    for item in items:
        item_id = item["id"]
        prompt = build_prompt(tokenizer, item["text"])
        try:
            result = generate(
                model,
                tokenizer,
                prompt=prompt,
                verbose=False,
                max_tokens=60,
                sampler=make_sampler(temp=0.0),
            )
        except Exception as exc:
            fail(f"MLX generation failed for item {item_id}: {exc}", 4)
        summaries[item_id] = clean_summary(result)

    json.dump({"summaries": summaries}, sys.stdout, ensure_ascii=False)


if __name__ == "__main__":
    main()
