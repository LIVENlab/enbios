"""
this is a stupid hack to include requirements.txt in the pyproject.toml file
cuz dependencies = {file = ["requirements.txt"]} does not include anything
"""

output: list[str] = []

with open("../requirements.txt") as fin:
    requirements = [f'"{r.strip()}"' for r in list(fin)]

# print(requirements)
# check first parameter

with open("../pyproject.toml", "r") as f:
    dependencies_detected = False
    for line in f:
        if line.startswith("dependencies"):
            dependencies_detected = True
            output.append(f"dependencies = [\n{', '.join(requirements)}\n]\n")
        elif dependencies_detected:
            if "]" in line:
                dependencies_detected = False
        else:
            output.append(line)

# print("".join(output))

with open("../pyproject.toml", "w") as f:
    f.writelines(output)
