from LXGREP import ReplicateSDE2GDB

if __name__ == "__main__":
    params = {
        "sde_instance": "KUCHINGLXG",
        "sde_username": "sde",
        "sde_password": "sde",
        "file_gdb": "cms_replica.gdb",
        "wildcard_datasets": "*CMS_ADMINISTRATIVE*",
        "wildcard_featureclass": ""}

    ReplicateSDE2GDB(**params)
