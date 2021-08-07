import os
from pathlib import Path
from typing import Union


def convert_path_from_docker(path: Union[Path, str]) -> Union[Path, str]:
    path_as_text = str(path)
    helix_pipeline_path = os.environ.get("HELIX_PIPELINE_PATH") or ""
    print(f"helix_pipeline_path: {helix_pipeline_path}")
    if helix_pipeline_path and not helix_pipeline_path.endswith("/"):
        helix_pipeline_path += "/"

    # in docker we run from a different path.  Change it so we can open the files in Terminal window
    if path_as_text.startswith("/helix.pipelines/"):
        replaced_path = path_as_text.replace(
            "/helix.pipelines/", str(helix_pipeline_path)
        )
        print(f"replaced_path1: {replaced_path}")
        return replaced_path
    elif path_as_text.startswith("/opt/project/"):
        replaced_path = path_as_text.replace("/opt/project/", str(helix_pipeline_path))
        print(f"replaced_path2: {replaced_path}")
        return replaced_path
    else:
        return path
