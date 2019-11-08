
import dxpy


def create_folder(container, folder_name):
    try:
        container.new_folder(folder_name, parents=True)
    except dxpy.exceptions.InvalidState:
        print(
            "Folder Exists %s ignoring new_folder creation" % folder_name
        )


@dxpy.entry_point('main')
def main(output_file, output_config_file, ___reorg_config=None, ___reorg_status=None):

    output_file = dxpy.DXFile(output_file)
    output_config_file = dxpy.DXFile(output_config_file)

    output_folder_1 = '/tests/test_reorg/out_1'
    output_folder_2 = '/tests/test_reorg/out_2'

    dx_container = dxpy.DXProject(dxpy.PROJECT_CONTEXT_ID)

    create_folder(dx_container, output_folder_1)
    create_folder(dx_container, output_folder_2)

    dx_container.move(
        destination=output_folder_1,
        objects=[output_file.get_id()]
    )

    dx_container.move(
        objects=[output_config_file.get_id()],
        destination=output_folder_2,
    )

    return {
        "outputs": [
            output_file, output_config_file
        ]
    }
