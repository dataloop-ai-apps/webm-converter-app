import traceback
import numpy as np
import dtlpy as dl
import datetime
import logging
import shutil
import time
import os

# from mail_handler import MailHandler
import video_utilities

logger = logging.getLogger(__name__)
NUM_RETRIES = 2


class ConversionMethod:
    FFMPEG = 'ffmpeg'
    OPENCV = 'opencv'


class WebmConverter(dl.BaseServiceRunner):
    """
    Plugin runner class

    """

    def __init__(self, method=None, remote_path=None):
        if not method:
            method = ConversionMethod.FFMPEG
        # self.mail_handler = MailHandler(service_name='custom-webm-converter')
        if remote_path is None:
            self.remote_path = '/.dataloop/webm'
        else:
            self.remote_path = remote_path
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

    def convert_to_webm_opencv(self, item, dir_path, nb_streams):
        """
        Convert and Save the item file in webm format by opencv

        :param dl.item item: the item object of the file
        :param str dir_path: the dir that have the input and output files
        :param int nb_streams: the number if streams of the file example (nb_streams=2 when the video have an audio)
        """
        output_file_path = os.path.join(dir_path, '{}.webm'.format(item.id))
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
            webm_audio = os.path.join(dir_path, '{}.aac'.format(item.id))
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
                               nb_frames=None,
                               progress=None):
        """
        Convert and Save the item file in webm format by ffmpeg

        :param str input_filepath: the file path to convert
        :param str output_filepath: the output file path
        :param int fps: the fps of the file (Frames per second)
        :param int nb_frames: the number of frames of the file
        :param dl.Progress progress: progress object to follow the work progress
        """
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
        video_utilities.execute_cmd(cmd=cmds, nb_frames=nb_frames, progress=progress)

        return

    def _upload_webm_item(self, item, webm_file_path):
        """
        Upload the webm file to the platform

        :param dl.item item: the item object of the file
        :param str webm_file_path: the webm file (output file of the converter method)
        :return: the uploaded item
        """
        dataset = dl.datasets.get(fetch=False, dataset_id=item.datasetId)
        pre, _ = os.path.splitext(item.filename)
        item_arr = pre.split('/')[:-1]
        item_folder = '/'.join(item_arr)

        remote_path = f'{self.remote_path}/{item_folder}'
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
        d = datetime.datetime.utcnow()
        epoch = datetime.datetime(1970, 1, 1)
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
        if not success:
            video_utilities.update_item_errors(item=item, error_dicts=err_dict)
        return success, summary
    
    @staticmethod
    def _verify_vme(item: dl.Item):
        # if metadata in the item no need to extract it
        if 'ffmpeg' not in item.metadata['system'] or 'nb_read_frames' not in item.metadata['system']['ffmpeg']:
            filters = dl.Filters(resource=dl.FiltersResource.EXECUTION)
            filters.add(field = 'input.item.item_id', values=item.id)
            execution = dl.executions.list(filters=filters)[0]
            execution : dl.Execution = execution
            if execution.in_progress():
                execution = execution.wait()
            if execution.status == dl.ExecutionStatus.FAILED:
                execution = execution.rerun()
                execution = execution.wait()
            if execution.status == dl.ExecutionStatus.SUCCESS:
                item = dl.items.get(item_id=item.id)
                if 'ffmpeg' not in item.metadata['system'] or 'nb_read_frames' not in item.metadata['system']['ffmpeg']:
                    raise Exception("Failed to extract metadata from VME, Execution ID: {}".format(execution.id))
            else:
                raise Exception("Failed to extract metadata from VME, Execution ID: {}".format(execution.id))
        orig_metadata = {
            'ffmpeg': item.metadata['system']['ffmpeg'],
            'start_time': item.metadata.get('startTime', 0),
            'height': item.height,
            'width': item.width,
            'fps': item.metadata.get('system', {}).get('fps', None),
            'nb_streams': item.metadata['system'].get('nb_streams', 1)
        }

        if item.metadata['system'].get('duration', None) is not None:
            orig_metadata['duration'] = float(item.metadata['system']['duration'])

        if item.metadata['system']['ffmpeg'].get('nb_read_frames', None) is not None:
            orig_metadata['nb_read_frames'] = int(item.metadata['system']['ffmpeg']['nb_read_frames'])

        if item.metadata['system']['ffmpeg'].get('nb_frames', None) is not None:
            orig_metadata['nb_frames'] = int(item.metadata['system']['ffmpeg']['nb_frames'])
        return orig_metadata
        
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
        log_header = '[preprocess][on_create][{item_id}][{func}]'.format(item_id=item.id, func='webm-converter')
        webm_filepath = os.path.join(workdir, '{}.webm'.format(item.id))
        orig_filepath = os.path.join(workdir, item.name)
        orig_filepath = item.download(local_path=orig_filepath)

        orig_metadata = self._verify_vme(item=item)
        logger.info('{header} downloading item'.format(header=log_header))

        logger.info('{} converting with {}'.format(log_header, self.method))
        valid_data, msg = video_utilities.validate_metadata(metadata=orig_metadata)
        if not valid_data:
            return valid_data, msg
        tic = time.time()
        if self.method == ConversionMethod.FFMPEG:
            self.convert_to_webm_ffmpeg(
                input_filepath=orig_filepath,
                output_filepath=webm_filepath,
                fps=orig_metadata['fps'],
                nb_frames=orig_metadata.get('nb_read_frames', None),
                progress=progress
            )
        elif self.method == ConversionMethod.OPENCV:
            self.convert_to_webm_opencv(
                item=item,
                dir_path=workdir,
                nb_streams=orig_metadata.get('nb_streams', 1))
        else:
            raise Exception(" unsupported converter method")

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
                                                                            default_start_time=summary[
                                                                                'webm_start_time'],
                                                                            prefix_check='web')
        if not validate:
            video_utilities.update_item_errors(item=item, error_dicts=validate_msg)
            video_utilities.send_error_event(item)

        logger.info(
            '{header} converted with {method}. conversion took: {dur}[s]'.format(
                header=log_header,
                method=self.method,
                dur=duration
            )
        )

        # upload web to platform
        webm_item = self._upload_webm_item(
            item=item,
            webm_file_path=webm_filepath
        )

        if not isinstance(webm_item, dl.Item):
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
            for _ in range(NUM_RETRIES):
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
                    msg = traceback.format_exc()
                    continue

            if not success:
                raise Exception(msg)

        except Exception as e:
            if 'Invalid data found when processing input' in str(e):
                e = "Failed to convert to webm because the downloaded file is corrupted."
            # self.mail_handler.send_alert(item=item, msg=str(e))
            raise ValueError('[webm-converter] failed\n error: {}'.format(e))
        finally:
            if workdir is not None and os.path.isdir(workdir):
                shutil.rmtree(workdir)
