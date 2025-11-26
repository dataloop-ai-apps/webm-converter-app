import dtlpy as dl
import os

package_name = 'custom-webm-converter'
project_name = 'projectName'

project = dl.projects.get(project_name=project_name)

###############
#   package   #
###############

module = [
    dl.PackageModule(
        name='webm_module',
        class_name='WebmConverter',
        entry_point='webm_converter.py',
        init_inputs=[dl.FunctionIO(type=dl.PackageInputType.STRING, name="method")],
        functions=[
            dl.PackageFunction(
                inputs=[dl.FunctionIO(type=dl.PackageInputType.ITEM, name="item")],
                outputs=[dl.FunctionIO(type=dl.PackageInputType.ITEM, name="item")],
                name='run',
                description='Run Webm converter on input item, except method as param, possible values: ffmpeg, opencv. default to ffmpeg'),
        ]
    )
]

# deploy the package using your local source code
# package = project.packages.push(
#     package_name=package_name,
#     modules=module,
#     src_path=os.getcwd()
# )

# deploy the package using our GIT repo
package = project.packages.push(
    package_name=package_name,
    modules=module,
    service_config={
        'runtime': dl.KubernetesRuntime(
            concurrency=1,
            pod_type=dl.InstanceCatalog.REGULAR_S,
            runner_image='gcr.io/viewo-g/piper/agent/cpu/webm:8',
            autoscaler=dl.KubernetesRabbitmqAutoscaler(
                min_replicas=1,
                max_replicas=2,
                queue_length=2
            )).to_json()},
    codebase=dl.GitCodebase(git_url='https://github.com/dataloop-ai/webm_converter.git', git_tag='main')
)

package = project.packages.get(package_name=package_name)

###########
# service #
###########
# deploy a new service
service = package.services.deploy(
    init_input={'method': 'ffmpeg'},
    service_name=package_name,
    execution_timeout=2 * 60 * 60,
    module_name=module[0].name,
)

service = project.services.get(service_name=package.name.lower())

if package.version != service.package_revision:
    service.package_revision = package.version
    service = service.update(True)

#########################
# new trigger creation #
#######################

triggers = service.triggers.list()
if triggers.items_count < 1:
    # adding trigger, run on items with mimetype video, size less than 0.5GB
    trigger = project.triggers.create(
        name=package.name,
        service_id=service.id,
        execution_mode=dl.TriggerExecutionMode.ONCE,
        resource='Item',
        actions=['Updated'],
        filters={
            '$and': [
                {
                    'metadata.system.mimetype': {
                        '$eq': 'video*'
                    }
                },
                {
                    'metadata.system.size': {
                        '$lt': 536870912
                    }
                },
                {
                    'metadata.system.mimetype': {
                        '$ne': 'video/webm'
                    }
                },
                {
                    "metadata.system.fps": {
                        "$gt": 0
                    }
                },
                {
                    "hidden": False
                },
                {
                    "type": "file"
                }
            ]
        }
    )
else:
    trigger = triggers.items[0]
