import math
from copy import deepcopy

import dtlpy as dl
import subprocess
import logging
import json

logger = logging.getLogger(__name__)
NUM_TRIES_COMMAND = 1


def execute_cmd(cmd, progress: dl.Progress = None, nb_frames=None):
    """
    execute bash command

    :param list cmd: list of the bash command
    :param dl.Progress progress: progress object, to follow thw work Progress
    :param int nb_frames: number of frames

    :return: the command output
    """
    exception = ''
    progress_conv = 10
    for _ in range(NUM_TRIES_COMMAND):
        if progress is not None and nb_frames is not None:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
            while proc.poll() is None:
                line = proc.stdout.readline()
                if progress is not None:
                    if 'frame=' in line:
                        frames = int(line.split('=')[1].split('f')[0].strip())
                        if frames / nb_frames >= progress_conv / 100:
                            progress.update(progress=progress_conv)
                            progress_conv += 10
        else:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        outs, errs = proc.communicate()

        if proc.returncode == 0:
            # if success return the result
            logger.debug(outs)
            return outs
        else:
            # if fail - keep error and try again
            exception += 'stderr:{}'.format(errs)
    # if all tries fail - raise the exception
    raise Exception(exception)


def extract_metadata(item_stream, with_headers=False):
    """
    build and run the command to extract metadata

    :param str item_stream: item stream
    :param bool with_headers: url item or regular

    :return: the command output
    """
    if with_headers:
        cmd = ['ffprobe',
               '-select_streams',
               'v:0',
               '-hide_banner',
               '-count_frames',
               '-count_packets',
               '-show_format',
               '-show_streams',
               '-of',
               'json',
               '-headers',
               "{}:{}".format('authorization', dl.client_api.auth['authorization']),
               item_stream]
    else:
        # without headers
        cmd = ['ffprobe',
               '-select_streams',
               'v:0',
               '-count_frames',
               '-count_packets',
               '-show_format',
               '-show_streams',
               '-of',
               'json',
               item_stream]

    return execute_cmd(cmd=cmd)


def duration_str_to_sec(time_str):
    """Get Seconds from time."""
    if time_str is None:
        return None
    try:
        h, m, s = time_str.split(':')
        return int(h) * 3600 + int(m) * 60 + float(s)
    except Exception:
        logger.exception("Got unsupported time_str: {!r}".format(time_str))
        return None


def metadata_extractor_from_ffmpeg(stream, with_headers):
    """
    get the item metadata from ffmpeg

    :param str stream: item stream
    :param bool with_headers: url item or regular

    :return: dict of the metadata
    """
    outs = extract_metadata(
        item_stream=stream,
        with_headers=with_headers
    )

    probe_result = json.loads(outs.decode('utf-8'))
    video_stream = next((stream for stream in probe_result['streams'] if stream['codec_type'] == 'video'), None)

    nb_streams = probe_result.get('format', {}).get('nb_streams', 1)

    if video_stream is None:
        raise ValueError('missing video stream for: {}'.format(stream))

    start_time = video_stream.get('start_time', None)
    start_time = eval(start_time) if start_time is not None else 0

    height = video_stream.get('height', None)
    width = video_stream.get('width', None)

    fps = video_stream.get('avg_frame_rate', None)
    fps = eval(fps) if fps is not None else None

    nb_frames = video_stream.get('nb_frames', None)
    nb_frames = eval(nb_frames) if nb_frames is not None else None

    nb_read_frames = video_stream.get('nb_read_frames', None)
    nb_read_frames = eval(nb_read_frames) if nb_read_frames is not None else None

    if 'duration' not in video_stream:
        tags = video_stream.get("tags", dict())
        duration = duration_str_to_sec(tags.get("DURATION", None))
    else:
        duration = video_stream.get('duration', None)
        duration = eval(duration) if duration is not None else None

    if duration is None:
        video_format = probe_result.get('format', dict())
        duration = video_format.get('duration', None)

    res_dict = {
        'ffmpeg': video_stream,
        'start_time': start_time,
        'height': height,
        'width': width,
        'fps': fps,
        'duration': float(duration) if duration is not None else None,
        'nb_read_frames': nb_read_frames,
        'nb_streams': nb_streams
    }
    if nb_frames is not None:
        res_dict['nb_frames'] = nb_frames
    return res_dict


def validate_video(fps, duration, r_frames, default_start_time=0, prefix_check='web'):
    if fps and duration and r_frames:
        if default_start_time is None:
            default_start_time = 0
        exp_frames_count = fps * float(int((duration - default_start_time) * 100)) / 100
        rounded = round(exp_frames_count)
        rounded_up = (math.floor(exp_frames_count) + 1)

        if rounded == rounded_up or rounded == r_frames:
            exp_frames = rounded
        else:
            exp_frames = rounded_up

        if exp_frames != r_frames and abs(exp_frames_count - r_frames) > 0.5:
            return False, exp_frames, error_dict(err_type=prefix_check + 'ExpectedFrames',
                                                 err_message='Frames is not equal to FPS * Duration',
                                                 err_value=abs(exp_frames_count - r_frames),
                                                 service_name='WebmConverter')
        return True, exp_frames, {}
    else:
        return True, 0, {}


def error_dict(err_type, err_message, err_value, service_name):
    """
    build the error dict format
    """
    return {
        'type': err_type,
        'message': err_message,
        'value': err_value,
        'service': service_name
    }


def send_error_event(item: dl.Item):
    """
    send error event
    """
    payload = {
        "notificationCode": "Platform.DataManagement.Item.ETL.ProcessFailed",
        "context": {"project": item.project.id, "org": item.project.org['id'], "dataset": item.dataset.id},
        "eventMessage": {"title": 'WebM Conversion Failed',
                         "description": f"One or more files finished conversion to WebM but may not be available for annotation work. This often results from Metadata missmatch, such as height-width information, or corrupted files. Investigate the files from enclosed links and resolve by adding new, correct files to the task."
                         },
        "priority": 100,
        "type": 'system',
        "body": {},
    }
    dl.client_api.gen_request(req_type='post',
                              path='/notifications/publish',
                              json_req=payload
                              )


def update_item_errors(item: dl.Item, error_dicts):
    """
    update the item metadata with the relevant errors

    :param dl.item item: the item object of the file
    :param list error_dicts: list of the errors
    """
    if not isinstance(error_dicts, list):
        error_dicts = [error_dicts]
    if 'system' not in item.metadata:
        item.metadata['system'] = {}
    if 'errors' not in item.metadata['system']:
        item.metadata['system']['errors'] = []
    for err_dict in error_dicts:
        add_err = True
        for err_index in range(len(item.metadata['system']['errors'])):
            if item.metadata['system']['errors'][err_index]['type'] == err_dict['type']:
                item.metadata['system']['errors'][err_index] = err_dict
                add_err = False
        if add_err:
            item.metadata['system']['errors'].append(err_dict)
    item.update(True)


def clean_item(item: dl.Item, service_name: str):
    if '{}_fail'.format(service_name) in item.metadata['system']:
        item.metadata['system'].pop('{}_fail'.format(service_name))

    if 'errors' in item.metadata['system']:
        errors = deepcopy(item.metadata['system'].get('errors'))
        for err in errors:
            if err.get('service', '') == service_name:
                item.metadata['system']['errors'].remove(err)


def validate_metadata(metadata):
    missing = [key for key, val in metadata.items() if
               not val and key in ['ffmpeg', 'height', 'width', 'fps', 'duration']]
    if 'nb_read_frames' not in metadata and 'nb_frames' not in metadata:
        missing.append('nb_read_frames')
    if len(missing) != 0:
        return False, 'missing metadata values {}'.format(missing)
    return True, ''
