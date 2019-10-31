
import dxpy


@dxpy.entry_point('main')
def main(output_file, output_config_file, config=None):

    output_file = dxpy.DXFile(output_file)
    output_config_file = dxpy.DXFile(output_config_file)
    config = dxpy.DXFile(config)

    output_folder_1 = '/tests/test_reorg/out_1'
    output_folder_2 = '/tests/test_reorg/out_2'

    dx_container = dxpy.DXProject(dxpy.PROJECT_CONTEXT_ID)
    dx_container.new_folder(output_folder_1, parents=True)
    dx_container.new_folder(output_folder_2, parents=True)

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
