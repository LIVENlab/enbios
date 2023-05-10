from processing.main import Enviro


cfg_file_path = "/home/ra/PycharmProjects/enbios2/data/AlexEnbios1/base.yaml"
just_prepare_base: bool = False
fragments_list_file: bool = False
first_fragment: int = 0
n_fragments: int = 0
max_lci_interfaces: int = 0
keep_min_fragment_files: bool = True
generate_nis_base_file: bool = False
generate_full_fragment_files: bool = False
generate_interface_results: bool = False
generate_indicators: bool = False
n_cpus: int = 1
log: str = None


t = Enviro()
t.set_cfg_file_path(cfg_file_path)

t.compute_indicators_from_base_and_simulation(n_fragments,
                                              first_fragment,
                                              generate_nis_base_file,
                                              generate_full_fragment_files,
                                              generate_interface_results,
                                              keep_min_fragment_files,
                                              generate_indicators,
                                              fragments_list_file,
                                              max_lci_interfaces,
                                              n_cpus,
                                              just_prepare_base)