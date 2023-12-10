import subprocess
import tempfile
from typing import List, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import yaml


def extract_container_image_refs_from_snapshot(snapshot_yaml: str) -> List[str]:
    """
    Extract container image references list from snapshot file
    :param snapshot_yaml: reference to snapshot file
    :return: list of container image references
    """
    with open(snapshot_yaml, 'r') as file:
        snapshot: yaml = yaml.safe_load(file)
    components = snapshot["spec"]["components"]
    container_images: List[str] = []
    for component in components:
        container_images.append(component["containerImage"])
    return container_images


def extract_rpm_db_from_container_image(container_image_ref: str) -> (str, str):
    """
    Extract rpm DB from a given container image reference
    Using OC image extract command
    :param container_image_ref: container image reference url
    :return: container_image_ref url, rpm folder url containing extracted rpm DB
    """
    rpm_folder: str = tempfile.mkdtemp()
    print(datetime.now().strftime("%H:%M:%S.%f")[
          :-3] + " - Extracting rpmDB from containerImage " + container_image_ref + " to " + rpm_folder + "\n")
    try:
        subprocess.run(
            ["oc", "image", "extract", container_image_ref, "--path", "/var/lib/rpm/:" + rpm_folder],
            capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        print("Failed extract image from " + container_image_ref + "\n" + e.stderr)
        return None

    print(datetime.now().strftime("%H:%M:%S.%f")[
          :-3] + " - containerImage " + container_image_ref + " rpmDB was extracted successfully\n")
    return container_image_ref, rpm_folder


def get_unsigned_rpms_from_rpmdb(rpm_db_folder: str) -> Optional[List[str]]:
    """
    Verify signed RPMs in a given rpm db folder
    :param rpm_db_folder: rpm db folder
    :return: list of unsigned rpms
    """
    try:
        r: subprocess.CompletedProcess[str] = subprocess.run(["rpm", "-qa", "--qf",
                                                              "%{NAME}-%{VERSION}-%{RELEASE} %{SIGPGP:pgpsig}\n",
                                                              "--dbpath", rpm_db_folder], capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print("Failed to run rpm query on " + rpm_db_folder + "\n" + e.stderr)
        return None

    unsigned_rpms: List[str] = []
    rpm_list: List[str] = r.stdout.split("\n")
    for rpm in rpm_list:
        if "Key ID" not in rpm and not rpm.startswith("gpg-pubkey"):
            unsigned_rpms.append(rpm.split(" ")[0])

    if unsigned_rpms == ['']:
        return None
    return unsigned_rpms


if __name__ == '__main__':

    container_image_refs: List[str] = extract_container_image_refs_from_snapshot("./snapshot.yaml")
    with ThreadPoolExecutor(max_workers=len(container_image_refs)) as executor:
        rpm_db_folders = executor.map(extract_rpm_db_from_container_image, container_image_refs)

    for rpm_db_folder in rpm_db_folders:
        if rpm_db_folder is not None:
            unsigned_rpms = get_unsigned_rpms_from_rpmdb(rpm_db_folder=rpm_db_folder[1])
            if unsigned_rpms is not None:
                print("Unsigned packages for container image " + rpm_db_folder[0])
                print(unsigned_rpms)
            else:
                print("All packages in container image " + rpm_db_folder[0] + " are signed")