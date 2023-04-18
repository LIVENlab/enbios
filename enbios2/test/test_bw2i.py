from enbios2.models.multi_scale_bw import BWSetup, BWDatabase

if __name__ == "__main__":
    from enbios2.bw2.bw2i import bw_setup
    bw_setup(setup=BWSetup(
        project_name="test",
        databases=[
            BWDatabase(
                name="test",
                source="test",
                format="test",
                importer="test"
            )]
    ))
