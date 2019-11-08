
import dxpy


def create_folder(container, folder_name):
    try:
        container.new_folder(folder_name, parents=True)
    except dxpy.exceptions.InvalidState:
        print(
            "Folder Exists %s ignoring new_folder creation" % folder_name
        )


@dxpy.entry_point('main')
def main(___reorg_config=None, ___reorg_status=None):

    job_describe = dxpy.describe(dxpy.DX_JOB_ID)
    analysis_id = job_describe["analysis"]

    stages = dxpy.describe(analysis_id)["stages"]

    # key is the name of the output and the value is the link of the file.
    output_map = [x['execution']['output'] for x in stages if x['id'] == 'stage-outputs'][0]

    output_file = output_map.get('output_file')
    output_config_file = output_map.get('output_config_file')

    output_folder_1 = '/tests/test_reorg/out_1'
    output_folder_2 = '/tests/test_reorg/out_2'

    dx_container = dxpy.DXProject(dxpy.PROJECT_CONTEXT_ID)

    dx_container.move(
        destination=output_folder_1,
        objects=[output_file]
    )
    dx_container.move(
        objects=[output_config_file],
        destination=output_folder_2,
    )

    return {
        "outputs": [
            output_file, output_config_file
        ]
    }
