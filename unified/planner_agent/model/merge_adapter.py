"""
Merge the fine-tuned LoRA adapter into the full-precision Qwen2.5-7B-Instruct
base, producing a standalone HF model dir ready for GGUF conversion.

The adapter in planner_agent_lora.zip was trained on the 4-bit unsloth base
(unsloth/Qwen2.5-7B-Instruct-bnb-4bit). For a portable GGUF we merge it onto
the equivalent full-precision base Qwen/Qwen2.5-7B-Instruct instead.

Usage:
    python merge_adapter.py --adapter ./adapter --out ./merged
"""

import argparse

import torch
from peft import PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer

BASE_MODEL = "Qwen/Qwen2.5-7B-Instruct"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--adapter", default="./adapter", help="unzipped LoRA adapter dir")
    ap.add_argument("--out", default="./merged", help="output merged HF model dir")
    ap.add_argument("--base", default=BASE_MODEL, help="full-precision base model")
    args = ap.parse_args()

    print(f"Loading base model: {args.base}")
    base = AutoModelForCausalLM.from_pretrained(
        args.base,
        torch_dtype=torch.float16,
        device_map="cpu",
    )

    print(f"Attaching adapter: {args.adapter}")
    model = PeftModel.from_pretrained(base, args.adapter)

    print("Merging adapter weights into base...")
    model = model.merge_and_unload()

    print(f"Saving merged model -> {args.out}")
    model.save_pretrained(args.out, safe_serialization=True)

    # tokenizer (with the qwen-2.5 chat template) comes from the adapter dir
    tok = AutoTokenizer.from_pretrained(args.adapter)
    tok.save_pretrained(args.out)

    print("Done. Merged model ready for GGUF conversion.")


if __name__ == "__main__":
    main()
