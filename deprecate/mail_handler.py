import dtlpy as dl
import logging

logger = logging.getLogger(__name__)


class MailHandler(dl.BaseServiceRunner):
    def __init__(self, service_name: str):
        self.service_name = service_name

    def send_mail(self, email: str, item: dl.Item, msg: str):
        try:
            # noinspection PyProtectedMember
            executions = item.resource_executions.list()
            execution_id = ''
            for page in executions:
                for exe in page:
                    if exe.package_name == self.service_name:
                        execution_id = exe.execution_id
            dl.projects._send_mail(
                project_id=item.project_id,
                send_to=email,
                title='Dataloop WEBM Conversion failed on item :  {}'.format(item.id),
                content='A video file in your project failed the process of conversion into WEBM format.'
                        ' Please read the description below and correct the file as needed – it may contain corrupted headers,'
                        ' frames and metadata, causing the conversion process to fail and preventing annotation work on this item. '
                        '<br> Project: {} <br> Dataset: {} <br> Item ID: {} <br> Execution ID: {} <br> Item URL: {} <br> Failure message: {}'.format(
                    item.project.name,
                    item.dataset.name,
                    item.id,
                    execution_id,
                    item.platform_url,
                    msg
                )
            )
        except Exception:
            logger.exception('Failed to send mail to {}'.format(email))

    def send_alert(self, item: dl.Item, msg):
        try:
            item.metadata['system']['{}_fail'.format(self.service_name)] = msg
            item.update(system_metadata=True)
            for email in [
                item.creator
            ]:
                self.send_mail(email=email, item=item, msg=msg)
        except:
            logger.exception('Failed to send mail')
