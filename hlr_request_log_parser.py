import csv
import re
import argparse
from typing import Dict
from pathlib import Path


def arg_parse():
    parser = argparse.ArgumentParser(description='parse raw hlr log')
    parser.add_argument('summary', nargs='?', help='return summary for specified file')
    parser.add_argument('-f', '--file', required=True, help='raw hlr loh')
    parser.add_argument('parse', nargs='?', help='if provided parse log and write it in new csv file')
    return parser.parse_args()


def parse_log(file_in: str) -> Dict:
    resp = {}
    with open(file_in, 'r') as f:
        for line in f:
            if 'request started' in line:
                start_time, req_id, msisdn = re.search(
                    r'^(.*): request started. Request ID: (\d*).*DNIS: (\d*)',
                    line).groups()
                resp[req_id] = {
                    'start_time': start_time,
                    'msisdn': msisdn,
                    'mccmnc': None,
                    'result': None,
                    'proc_time': None,
                    'ported': None,
                    'cached': None,
                    'message': None,
                }
            if 'request ended' in line:
                search = re.search(
                    r'.*Request ID: (\d*);.*MCCMNC: (\d{0,6}); '
                    r'result: (.*); proctime: (\d*\.\d*);.*'
                    r'ported: (\d*); cached: (\d*);.*message: (.*)',
                    line)
                req_id, mccmnc, result, proc_time, ported, cached, message = search.groups()
                resp[req_id]['mccmnc'] = mccmnc
                resp[req_id]['result'] = result
                resp[req_id]['proc_time'] = proc_time
                resp[req_id]['ported'] = ported
                resp[req_id]['cached'] = cached
                resp[req_id]['message'] = message
    return resp


def write_log(file_out, resp):
    with open(file_out, 'w') as f:
        csv_writer = csv.writer(f, delimiter=';')
        rows = [
            'start_time',
            'msisdn',
            'mccmnc',
            'result',
            'proc_time',
            'ported',
            'cached',
            'message'
        ]
        csv_writer.writerow(rows)
        for req_id in resp.keys():
            csv_writer.writerow([
                resp[req_id].get('start_time'),
                resp[req_id].get('msisdn'),
                resp[req_id].get('mccmnc'),
                resp[req_id].get('result'),
                resp[req_id].get('proc_time'),
                resp[req_id].get('ported'),
                resp[req_id].get('cached'),
                resp[req_id].get('message'),
            ])


def provide_resp_summary(resp: Dict[str, Dict]):
    total_request = len(resp)
    resp_details = [value for value in resp.values()]
    failed_request = len(
        list(filter(lambda x: x['result'] == '-1', resp_details)))
    cached_request = len(
        list(filter(lambda x: x['cached'] == '1', resp_details)))
    min_proc_time = min(
        [float(detail.get('proc_time')) for detail in resp_details
         if detail.get('proc_time') is not None and detail.get('cached') == '0'])
    max_proc_time = max(
        [float(detail.get('proc_time')) for detail in resp_details
         if detail.get('proc_time') is not None])
    without_resp = list(filter(lambda x: x['result'] is None, resp_details))
    requests_without_response_detail = [(resp.get('start_time'), resp.get('msisdn')) for resp in without_resp]
    resp_time_th1 = 0
    resp_time_th2 = 0
    resp_time_th3 = 0
    for resp in resp_details:
        if resp['cached'] == '0' and resp['result']:
            if float(resp['proc_time']) <= 7:
                resp_time_th1 += 1
            elif 7 < float(resp['proc_time']) <= 10:
                resp_time_th2 += 1
            else:
                resp_time_th3 += 1
    print(f'{total_request=}')
    print(f'{failed_request=}')
    print(f'{cached_request=}')
    print(f'{min_proc_time=}')
    print(f'{max_proc_time=}')
    print(f'requests_without_response={len(without_resp)}')
    print(f'{requests_without_response_detail=}')
    print(f'{resp_time_th1=}')
    print(f'{resp_time_th2=}')
    print(f'{resp_time_th3=}')


def main():
    args = arg_parse()
    f_in = args.file
    request_log = parse_log(f_in)
    if args.summary:
        provide_resp_summary(request_log)
    if args.parse:
        f_in_path = Path(f_in)
        f_out = Path.joinpath(f_in_path.parent, f'{f_in_path.stem}.csv')

        write_log(file_out=f_out, resp=request_log)
        print(f'log parsed in {f_out}')


if __name__ == '__main__':
    main()
