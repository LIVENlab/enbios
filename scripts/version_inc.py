import re
import sys

output: list[str] = []


# check first parameter
revert = len(sys.argv) > 1 and sys.argv[1] == "revert"

with open("../pyproject.toml", "r") as f:
    for line in f:
        if line.startswith("version"):
            # get "<digitis>.<digits>.<digits>" with regex
            # increment the last digit
            # replace the line with the new version
            version_pattern = r"\d+\.\d+\.\d+"
            version_s = re.findall(version_pattern, line)[0]
            parts = version_s.split(".")
            minor_num = int(parts[-1])
            new_minor_num = minor_num + (-1 if revert else 1)
            new_version = ".".join(parts[:-1] + [str(new_minor_num)])
            print(new_version)
            new_line = re.sub(version_pattern, new_version, line)
            output.append(new_line)
        else:
            output.append(line)

with open("../pyproject.toml", "w") as f:
    f.writelines(output)
