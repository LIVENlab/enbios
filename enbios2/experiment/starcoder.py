from huggingface_hub import login
from transformers import AutoModelForCausalLM, AutoTokenizer

from enbios2.settings import settings

from datasets import list_datasets, load_dataset
import datasets
# Print all the available datasets
# print(list_datasets())
# TODO WHERE TO?
# ds = load_dataset("ArmelR/stack-exchange-instruction") # home/.cache/hugging_face
ds = datasets.load_dataset("ArmelR/stack-exchange-instruction", split="train", streaming=True)

#
# login(settings.huggingface_token)
# checkpoint = "bigcode/starcoder"
# device = "cpu"  # for GPU usage or "cpu" for CPU usage
#
# tokenizer = AutoTokenizer.from_pretrained(checkpoint)
# # to save memory consider using fp16 or bf16 by specifying torch.dtype=torch.float16 for example
# model = AutoModelForCausalLM.from_pretrained(checkpoint).to(device)
#
# inputs = tokenizer.encode("def print_hello_world():", return_tensors="pt").to(device)
# outputs = model.generate(inputs)
# print(tokenizer.decode(outputs[0]))
