class ACXGenerator:

    def __init__(self, path_name):
        self.path_name = path_name

    def generate(self, inventory_name, data_sources_list, pk_list):
        for i in inventory_name:
            lista_pk = [pk[1] for pk in pk_list if pk[0] == i]
            for ds in data_sources_list:
                if ds[0] == i:
                    with open(f"{self.path_name}/{i.lower()}.acx", "a") as arquivo:
                        arquivo.write(f"SEGNAME={ds[1]},\n")
                        arquivo.write(f"    TABLENAME={ds[4].lower()}.{ds[1].lower()},\n")
                        arquivo.write(f"    CONNECTION={ds[4].upper()},\n")
                        arquivo.write(f"    KEY={'/'.join(lista_pk)}, $")