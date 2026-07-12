import traceback
import numpy as np
import dtlpy as dl
import datetime
import logging
import shutil
import time
import cv2
import os
# from mail_handler import MailHandler
import video_utilities
from service_config_manager import ServiceConfigManager

logger = logging.getLogger(__name__)
NUM_RETRIES = 2


class ConversionMethod:
    FFMPEG = 'ffmpeg'
    OPENCV = 'opencv'


class WebmConverter(dl.BaseServiceRunner):
    """
    Plugin runner class

    """
    
    # Default configuration values - used when no config file exists or keys are missing
    DEFAULTS = {
        "webm_upload_path": "/.dataloop/webm",
        "conversion_method": "ffmpeg"
    }

    def __init__(self, method=None):
        if not method:
            method = ConversionMethod.FFMPEG
        # self.mail_handler = MailHandler(service_name='custom-webm-converter')
        self.method = method
        if method == ConversionMethod.OPENCV:
            cmd_build_file = ['chmod', '777', 'opencv4_converter']
            video_utilities.execute_cmd(cmd=cmd_build_file)
        new_env = os.environ.get('INTERNAL_GATE_URL', None)
        if new_env and new_env not in dl.client_api.environment:
            url = dl.client_api.environment
            current_env = dl.environment().split('.')[0].split('//')[1]
            dl.client_api.environment = dl.client_api.environment.replace(current_env, new_env)
            dl.client_api.environment = dl.client_api.environment.replace('https', 'http')
            dl.client_api.add_environment(environment=dl.client_api.environment,
                                          alias=new_env,
                                          url=url)
        
        self.config_manager = ServiceConfigManager(
            service_name="webm-converter",
            defaults=self.DEFAULTS
        )

    def convert_to_webm_opencv(self, item, dir_path, nb_streams):
        """
        Convert and Save the item file in webm format by opencv

        :param dl.item item: the item object of the file
        :param str dir_path: the dir that have the input and output files
        :param int nb_streams: the number if streams of the file example (nb_streams=2 when the video have an audio)
        """
        output_file_path = os.path.join(dir_path, f'{item.id}.webm')
        input_file_path = os.path.join(dir_path, item.name)

        # start extract the video
        webm_video = str(os.path.join(dir_path, 'video.webm'))
        if os.path.isfile(webm_video):
            os.remove(webm_video)

        cmd = [
            './opencv4_converter',
            input_file_path,
            webm_video
        ]
        video_utilities.execute_cmd(cmd=cmd)

        if nb_streams == 2:
            have_audio = True
            # start extract the audio
            webm_audio = os.path.join(dir_path, f'{item.id}.aac')
            try:
                cmd = [
                    'ffmpeg',
                    '-i',
                    input_file_path,
                    '-vn',  # is no video.
                    '-acodec',
                    'copy',  # -acodec copy says use the same audio stream that's already in there.
                    '-y',
                    webm_audio
                ]
                video_utilities.execute_cmd(cmd=cmd)
            except Exception as err:
                if 'does not contain any stream' in str(err):
                    have_audio = False
                    pass
                else:
                    raise err

            # marge video and audio file into one webm file
            if have_audio:
                cmd = [
                    'ffmpeg',
                    '-i',
                    webm_video,
                    '-i',
                    webm_audio,
                    '-c:v',
                    'copy',  # copy video as ot with out encode
                    '-c:a',
                    'libopus',  # encode audio
                    '-map',
                    '0:0',
                    '-map',
                    '1:0',
                    '-hide_banner',
                    # -map 0:0 -map 1:0 - we map stream 0 (video) from first file, and stream 0 from second file (mp3) to output.
                    output_file_path
                ]
                video_utilities.execute_cmd(cmd=cmd)
            else:
                if os.path.isfile(output_file_path):
                    os.remove(output_file_path)
                os.rename(webm_video, output_file_path)
        else:
            if os.path.isfile(output_file_path):
                os.remove(output_file_path)
            os.rename(webm_video, output_file_path)

    def convert_to_webm_ffmpeg(self,
                               input_filepath,
                               output_filepath,
                               fps,
                               progress=None):
        """
        Convert and Save the item file in webm format by ffmpeg

        :param str input_filepath: the file path to convert
        :param str output_filepath: the output file path
        :param int fps: the fps of the file (Frames per second)
        :param int nb_frames: the number of frames of the file
        :param dl.Progress progress: progress object to follow the work progress
        """
        cap = cv2.VideoCapture(input_filepath)
        n_frames = cap.get(cv2.CAP_PROP_FRAME_COUNT)
        cap.release()
        n_frames = max(int(n_frames), 0) if n_frames and n_frames > 0 else None
        cmds = [
            'ffmpeg',
            # To force the frame rate of the output file
            '-r', str(fps),
            # Item local path / stream
            '-i', input_filepath,
            # Overwrite output files without asking
            '-y',
            '-hide_banner',
            # Log level
            '-v', 'info',
            # Duplicate or drop input frames to achieve constant output frame rate fps.
            '-max_muxing_queue_size', '9999',
            output_filepath
        ]
        video_utilities.execute_cmd(cmd=cmds, progress=progress, n_frames=n_frames)

        return

    @staticmethod
    def _upload_webm_item(item, webm_file_path, webm_upload_path: str = "/.dataloop/webm"):
        """
        Upload the webm file to the platform

        :param dl.item item: the item object of the file
        :param str webm_file_path: the webm file (output file of the converter method)
        :param str webm_upload_path: the base remote path for webm uploads (from config)
        :return: the uploaded item
        """
        dataset = dl.datasets.get(fetch=False, dataset_id=item.datasetId)
        pre, _ = os.path.splitext(item.filename)
        item_arr = pre.split('/')[:-1]
        item_folder = '/'.join(item_arr)

        remote_path = f'{webm_upload_path}{item_folder}'
        webm_item = dataset.items.upload(
            local_path=webm_file_path,
            remote_path=remote_path,
            overwrite=True
        )

        return webm_item

    @staticmethod
    def _set_item_modality(item: dl.Item, modality_item):
        """
        set the item modality

        :param dl.item item: the item object of the file
        :param dl.item modality_item: the webm item
        :return: the uploaded item
        """
        d = datetime.datetime.now(datetime.timezone.utc)
        epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        now = (d - epoch).total_seconds() * 1000
        item.modalities.create(
            modality_type='replace',
            ref=modality_item.id,
            ref_type=dl.MODALITY_REF_TYPE_ID,
            name=modality_item.name,
            timestamp=int(now)
        )
        item.update(system_metadata=True)
        item.dataset.items.update(filters=dl.Filters(field='spec.parentDatasetItemId',
                                                     values=item.id, use_defaults=False),
                                  system_update_values={'modalities': item.metadata['system'].get('modalities', [])},
                                  system_metadata=True)

    def verify_webm_conversion(self, webm_filepath: str, orig_metadata: dict, item=None):
        """
        Check and add validation to the webm output

        :param str webm_filepath: the webm file (output file of the converter method)
        :param dict orig_metadata: dict of the original file metadata
        :param dl.item item: the item object of the file
        """
        webm_ffprobe = video_utilities.metadata_extractor_from_ffmpeg(stream=webm_filepath, with_headers=False)

        webm_nb_read_frames = webm_ffprobe.get('nb_read_frames', None)
        if webm_nb_read_frames is None:
            webm_nb_read_frames = webm_ffprobe.get('nb_frames', None)
        webm_nb_read_frames = int(webm_nb_read_frames) if webm_nb_read_frames is not None else None

        orig_nb_read_frames = orig_metadata.get('nb_read_frames', None)
        if orig_nb_read_frames is None:
            orig_nb_read_frames = orig_metadata.get('nb_frames', None)
        orig_nb_read_frames = int(orig_nb_read_frames) if orig_nb_read_frames is not None else None

        webm_fps = webm_ffprobe['fps']
        orig_fps = orig_metadata['fps']

        webm_start_time = webm_ffprobe['start_time']
        orig_start_time = orig_metadata['start_time']

        webm_duration = webm_ffprobe['duration'] - webm_start_time
        orig_duration = orig_metadata['duration'] - orig_start_time

        summary = {
            'webm_nb_read_frames': webm_nb_read_frames,
            'orig_nb_read_frames': orig_nb_read_frames,
            'webm_fps': webm_fps,
            'orig_fps': orig_fps,
            'webm_duration': webm_duration,
            'orig_duration': orig_duration,
            'webm_start_time': webm_start_time,
            'orig_start_time': orig_start_time
        }

        diff_fps = np.abs(orig_fps - webm_fps) if orig_fps is not None and webm_fps is not None else None

        success = True
        err_dict = []
        # check fps
        if diff_fps is not None and diff_fps > 0.2:
            err_dict.append(video_utilities.error_dict(err_type='webFPSDiff',
                                                       err_message='Webm has different FPS from original video',
                                                       err_value=diff_fps,
                                                       service_name='WebmConverter'))

            success = False
        # check number of frames
        if orig_nb_read_frames is not None and webm_nb_read_frames is not None and orig_nb_read_frames != webm_nb_read_frames:
            err_dict.append(video_utilities.error_dict(err_type='webFrameDiff',
                                                       err_message='Webm has different frame number from original video',
                                                       err_value=abs(orig_nb_read_frames - webm_nb_read_frames),
                                                       service_name='WebmConverter'))
            success = False
        if not success and item is not None:
            video_utilities.update_item_errors(item=item, error_dicts=err_dict)
        return success, summary
    
    @staticmethod
    def _verify_vme(item: dl.Item):
        log_header = f'[_verify_vme][{item.id}]'
        system_metadata = item.metadata.get('system', {})
        has_ffmpeg = 'ffmpeg' in system_metadata
        has_nb_read_frames = has_ffmpeg and 'nb_read_frames' in system_metadata.get('ffmpeg', {})
        logger.info(f'{log_header} Checking VME metadata: has_ffmpeg={has_ffmpeg}, has_nb_read_frames={has_nb_read_frames}')

        # if metadata in the item no need to extract it
        if not has_ffmpeg or not has_nb_read_frames:
            logger.warning(f'{log_header} Missing VME metadata on item, attempting to find VME execution. '
                           f'system keys: {list(system_metadata.keys())}')
            filters = dl.Filters(resource=dl.FiltersResource.EXECUTION)
            filters.add(field = 'input.item.item_id', values=item.id)
            executions_page = dl.executions.list(filters=filters)
            if executions_page.items_count == 0:
                logger.error(f'{log_header} No VME executions found for item {item.id}')
                raise Exception(f"No VME executions found for item {item.id}. "
                                f"Ensure the Video Metadata Extractor service has processed this item.")
            execution = executions_page.items[0]
            execution : dl.Execution = execution
            logger.info(f'{log_header} Found VME execution {execution.id}, status={execution.latest_status.get("status", "unknown")}')
            if execution.in_progress():
                logger.info(f'{log_header} VME execution {execution.id} is in progress, waiting...')
                execution = execution.wait()
                logger.info(f'{log_header} VME execution {execution.id} finished with status={execution.latest_status.get("status", "unknown")}')
            if execution.status == dl.ExecutionStatus.FAILED:
                logger.warning(f'{log_header} VME execution {execution.id} failed, attempting rerun...')
                execution = execution.rerun()
                execution = execution.wait()
                logger.info(f'{log_header} VME execution rerun {execution.id} finished with status={execution.latest_status.get("status", "unknown")}')
            if execution.status == dl.ExecutionStatus.SUCCESS:
                item = dl.items.get(item_id=item.id)
                system_metadata = item.metadata.get('system', {})
                has_ffmpeg = 'ffmpeg' in system_metadata
                has_nb_read_frames = has_ffmpeg and 'nb_read_frames' in system_metadata.get('ffmpeg', {})
                logger.info(f'{log_header} Re-fetched item after VME execution. '
                            f'has_ffmpeg={has_ffmpeg}, has_nb_read_frames={has_nb_read_frames}, '
                            f'system keys: {list(system_metadata.keys())}')
                if not has_ffmpeg or not has_nb_read_frames:
                    ffmpeg_keys = list(system_metadata.get('ffmpeg', {}).keys()) if has_ffmpeg else []
                    logger.error(f'{log_header} VME execution {execution.id} succeeded but metadata still missing. '
                                 f'ffmpeg keys: {ffmpeg_keys}')
                    raise Exception(f"Failed to extract metadata from VME, Execution ID: {execution.id}")
            else:
                logger.error(f'{log_header} VME execution {execution.id} ended with unexpected status: {execution.latest_status}')
                raise Exception(f"Failed to extract metadata from VME, Execution ID: {execution.id}, "
                                f"status: {execution.latest_status}")

        ffmpeg_metadata = system_metadata.get('ffmpeg', {})
        start_time = system_metadata.get('startTime', 0)
        fps = system_metadata.get('fps', None)
        duration = system_metadata.get('duration', None)
        nb_streams = system_metadata.get('nb_streams', 1)
        nb_read_frames = ffmpeg_metadata.get('nb_read_frames', None)
        nb_frames = ffmpeg_metadata.get('nb_frames', None)

        orig_metadata = {
            'ffmpeg': ffmpeg_metadata,
            'start_time': start_time,
            'height': item.height,
            'width': item.width,
            'fps': fps,
            'nb_streams': nb_streams
        }

        if duration is not None:
            orig_metadata['duration'] = float(duration)
        else:
            logger.warning(f'{log_header} duration is missing from system metadata')

        if nb_read_frames is not None:
            orig_metadata['nb_read_frames'] = int(nb_read_frames)
        else:
            logger.warning(f'{log_header} nb_read_frames is missing from ffmpeg metadata')

        if nb_frames is not None:
            orig_metadata['nb_frames'] = int(nb_frames)
        else:
            logger.warning(f'{log_header} nb_frames is missing from ffmpeg metadata')

        if fps is None:
            logger.warning(f'{log_header} fps is missing from system metadata')
        if item.height is None or item.width is None:
            logger.warning(f'{log_header} height/width missing: height={item.height}, width={item.width}')

        logger.info(f'{log_header} Extracted orig_metadata: fps={fps}, duration={duration}, '
                    f'nb_read_frames={nb_read_frames}, nb_frames={nb_frames}, '
                    f'start_time={start_time}, nb_streams={nb_streams}, '
                    f'height={item.height}, width={item.width}')
        return orig_metadata, item
        
    def webm_converter(self,
                       item: dl.Item,
                       workdir,
                       progress=None,
                       ):
        """
        Convert to webm for web

        :param dl.item item: the item object of the file
        :param str workdir: the dir that have the input and output files
        :param progress: progress
        :return:
        """
        # Get resolved config for this dataset
        config = self.config_manager.get_config(
            dataset_id=item.datasetId
        )
        convert_method = config.get('conversion_method', self.method)
        logger.debug(f"Using config for dataset {item.datasetId}: {config}")
        
        log_header = f'[preprocess][on_create][{item.id}][webm-converter]'
        webm_filepath = os.path.join(workdir, f'{item.id}.webm')
        orig_filepath = os.path.join(workdir, item.name)
        orig_filepath = item.download(local_path=orig_filepath)

        orig_metadata, item = self._verify_vme(item=item)
        logger.info(f'{log_header} downloading item')
        logger.info(f'{log_header} converting with {self.method}')
        valid_data, msg = video_utilities.validate_metadata(metadata=orig_metadata)
        if not valid_data:
            logger.warning(f"Failed validating metata: {msg}")
            return valid_data, msg
        tic = time.time()
        if convert_method == ConversionMethod.FFMPEG:
            self.convert_to_webm_ffmpeg(
                input_filepath=orig_filepath,
                output_filepath=webm_filepath,
                fps=orig_metadata['fps'],
                progress=progress
            )
        elif convert_method == ConversionMethod.OPENCV:
            self.convert_to_webm_opencv(
                item=item,
                dir_path=workdir,
                nb_streams=orig_metadata.get('nb_streams', 1))
        else:
            raise Exception(f"unsupported converter method: {self.method}")

        duration = time.time() - tic
        same, summary = self.verify_webm_conversion(
            webm_filepath=webm_filepath,
            orig_metadata=orig_metadata,
            item=item
        )

        # check video correctness fps * duration == frames number
        validate, exp_frames, validate_msg = video_utilities.validate_video(fps=summary['webm_fps'],
                                                                            duration=summary['webm_duration'],
                                                                            r_frames=summary['webm_nb_read_frames'],
                                                                            default_start_time=summary['webm_start_time'],
                                                                            prefix_check='web')
        if not validate:
            video_utilities.update_item_errors(item=item, error_dicts=validate_msg)
            video_utilities.send_error_event(item)

        logger.info(f'{log_header} converted with {self.method}. conversion took: {duration}[s]')

        # upload web to platform
        webm_item = self._upload_webm_item(
            item=item,
            webm_file_path=webm_filepath,
            webm_upload_path=config.get("webm_upload_path", "/.dataloop/webm")
        )

        if not isinstance(webm_item, dl.Item):
            logger.error(f'Failed to upload webm. Uploaded item: {webm_item}')
            raise Exception('Failed to upload webm')

        # set modality on original
        self._set_item_modality(
            item=item,
            modality_item=webm_item
        )

        return True, ''

    def run(self, item: dl.Item, progress=None):
        ##################
        # webm converter #
        ##################
        workdir = None
        success = False
        msg = ''
        try:
            for i_try in range(NUM_RETRIES):
                logger.info(f'In run: try {i_try + 1}/{NUM_RETRIES}')
                try:
                    workdir = item.id
                    os.makedirs(workdir, exist_ok=True)
                    video_utilities.clean_item(item=item, service_name='WebmConverter')
                    success, msg = self.webm_converter(item=item, workdir=workdir, progress=progress)
                    if success:
                        break
                    else:
                        continue
                except Exception:
                    logger.error(f'failed in try: {i_try + 1}/{NUM_RETRIES}: {traceback.format_exc()}')
                    msg = traceback.format_exc()
                    continue

            if not success:
                raise Exception(msg)

        except Exception as e:
            if 'Invalid data found when processing input' in str(e):
                e = "Failed to convert to webm because the downloaded file is corrupted."
            # self.mail_handler.send_alert(item=item, msg=str(e))
            raise ValueError(f'[webm-converter] failed\n error: {e}')
        finally:
            if workdir is not None and os.path.isdir(workdir):
                shutil.rmtree(workdir)

    def on_delete(self, item: dl.Item):
        """
            Delete webm file if it exists
        """
        success = False
        item_modalities = item.metadata.get('system', {}).get('modalities', [])
        for modality in item_modalities:
            if modality.get('name').endswith('.webm'):
                webm_item = dl.items.get(item_id=modality.get('ref'))
                if webm_item is not None:
                    success = webm_item.delete()
                    if not success:
                        raise dl.exceptions.PlatformException('500', message=f"Failed to delete webm file, Item modalities: {item_modalities}")
        return success
if __name__ == '__main__':
    webm_converter = WebmConverter()
    webm_converter.run(item=dl.items.get(item_id=''))