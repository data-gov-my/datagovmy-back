def get_operation_files(operation):
    opr = operation.split(" ")
    chosen_opr = opr[0]
    files = []

    if len(opr) > 1:
        files = opr[1].split(",")

    return {"operation": chosen_opr, "files": files}
