def table2tree(csv_file: UploadFile,
               *,
               as_base: bool = True,
               destination: Optional[str] = None,
               delim: Optional[str] = None,
               include_columns: Optional[list] = (),
               ignore_columns: Optional[list] = (),
               remove_empty_children: bool = True,
               root_value: str = None,
               read_levels: bool = True):
    """
    @param csv_file:
    @param as_base: makes content either `value` or if set false, `text` (language entries)
    @param destination: json file. if None generate Tree object and return
    @param delim: use csv-table delimiter. If None (default) try and guess (",",";")
    @param include_columns:
    @param ignore_columns:
    @param remove_empty_children:
    @param root_value: value/text for root element
    @param read_levels: reads 2nd row as level-fields (value or text), default: True
    @return:
    """
    if not delim:
        reader = dict_reader_guess_delimiter(csv_file)
    else:
        reader = csv.DictReader(csv_file.file, delimiter=delim)

    all_field_names = reader.fieldnames
    # print(all_field_names)
    VALUE = "value"

    if include_columns:
        field_names = include_columns
        missing = list(filter(lambda col: col not in all_field_names, include_columns))
        if missing:
            print("defined columns do not exist in the table", missing)
            print("candidates are", all_field_names)
            return None

    else:
        field_names = list(filter(lambda fn: "/" not in fn and fn not in ignore_columns, all_field_names))
    # logger.debug(f"Taking columns: {field_names}")
    # print(f"Taking columns: {field_names}")

    columns_act = {col: None for col in field_names}
    basename = os.path.basename(csv_file).split(".")[0]
    root_value = root_value if root_value else basename
    root = {CHILDREN: [], VALUE if as_base else TEXT: root_value}
    values = {"root": root}  # levels come later for language
    if as_base:
        values["levels"] = [{VALUE: field} for field in field_names]

    additional_data_rows = ["description", "icon"]

    def add_at_index(index: int, value, additional_data: Dict, tag: str, row: Dict):
        act = root[CHILDREN]
        # print(f"{value} -> {index}")
        for i in range(index):
            try:
                act = act[-1][CHILDREN]
            except:
                print(f"failed to insert {value} at index {i}. 'act' is {act}")
        # print(json.dumps(root, indent=2))
        # print(i)
        if as_base:
            insert = {VALUE: value}
        else:
            insert = {TEXT: value}
        for additional_k, val in additional_data.items():
            insert[additional_k] = val

        if tag and as_base:
            insert["tag"] = tag

        # only at the last level
        if row.get("extra-type") and row.get("extra-name") and (index == len(field_names) - 1):
            if as_base:
                insert["extra"] = {"type": row["extra-type"], VALUE: row["extra-name"]}
            else:
                insert["extra"] = {TEXT: row["extra-name"]}

        # print(description)
        if index < len(field_names) - 1:
            # print("+kids", index, value)
            insert[CHILDREN] = []
        # print(insert)
        act.append(insert)

    if read_levels:
        levels = next(reader)
    if not as_base:
        values["levels"] = [{"text": levels[field], "description": levels[field + "/description"]} for field in
                            field_names]
    for row in reader:
        all_levels_of_row = [row[col].strip() for col in field_names if row[col].strip() != ""]

        for index, col in enumerate(field_names):
            val = row[col]
            # print(val, columns_act)
            if val != columns_act[col] and val.strip() != "":
                columns_act[col] = val
                # print("got col", col)
                for col_name in field_names[field_names.index(col) + 1:]:
                    # print("resetting", col_name)
                    columns_act[col_name] = None
                # here also reset the columns_act of the following columns. so when the same name appears (in one of the following cols)
                # they are accepts (val != columns_act[col] must pass)
                additional_data = {}
                for additionals in additional_data_rows:
                    if additionals in (base_fields if as_base else lang_fields):
                        col_name = f"{col}/{additionals}"
                        # print(col_name)
                        if col_name in all_field_names:
                            # print("found")
                            additional_data[additionals] = row[col_name]

                tag = None
                # print(val, val == all_levels_of_row[-1], row.get("tag"))
                if val == all_levels_of_row[-1]:  # leafnode
                    tag = row.get("tag")
                # print(val)
                # print("additionals", additional_data)
                add_at_index(index, val.strip(), additional_data, tag, row)

    if remove_empty_children:
        def rec_kids_search(node):
            if CHILDREN in node:
                if len(node[CHILDREN]) == 0:
                    del node[CHILDREN]
                else:
                    for kid in node[CHILDREN]:
                        rec_kids_search(kid)

        rec_kids_search(root)

    # print(values)
    tree = Tree.from_dict(values)
    if destination:
        json.dump(tree.dumps(), open(destination, "w", encoding="utf-8"))
    return tree
