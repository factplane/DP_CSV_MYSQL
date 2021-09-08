class checkpointer:
    import os

    CHK_POINT_FILE_NAME = "__checkpoint_file.ckpt"
    chk_point_file = None

    def __init__(self):
        try:
            ##opening an existing file
            self.chk_point_file = open(self.CHK_POINT_FILE_NAME, "rt+")
        except FileNotFoundError:
            ##creating and opening a new file
            self.chk_point_file = open(self.CHK_POINT_FILE_NAME, "wt+")

    def set_checkpoint(self, step, payload, startpoint, endpoint):
        ckpt_str = str(
            {"process": __file__, "step": step, "payload": payload, "startpoint": startpoint, "endpoint": endpoint})
        self.chk_point_file.writelines(ckpt_str)
        self.chk_point_file.writelines("\n")
        # print("checkpoint written")

    def clean_checkpoint(self):
        self.chk_point_file.close()
        self.os.remove(self.CHK_POINT_FILE_NAME)

    def get_last_checkpoint(self):
        last_line = ""
        self.chk_point_file.seek(0)
        for line in self.chk_point_file:
            last_line = line
            # print("debug" + line)
        return last_line

print("Successfully Imported Checkpointer")

if __name__ == "__main__":
    ## Simple use
    ##create checkpointer object
    cp = checkpointer()

    ##get last checkpoint
    print(cp.get_last_checkpoint())

    ##set new checkpoint
    cp.set_checkpoint("step1", "dataset1", 1, 1000)
    print(cp.get_last_checkpoint())

    ##Cleanup check points on a exit
    cp.clean_checkpoint()
