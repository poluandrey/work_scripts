import argparse
import csv
import pathlib
import re
from typing import Dict, List

COUNTRY_CODES = [
    "1",
    "1242",
    "1246",
    "1264",
    "1268",
    "1284",
    "1340",
    "1345",
    "1441",
    "1473",
    "1649",
    "1664",
    "1671",
    "1684",
    "1758",
    "1767",
    "1784",
    "1787",
    "1809",
    "1868",
    "1869",
    "1876",
    "20",
    "211",
    "212",
    "213",
    "216",
    "218",
    "220",
    "221",
    "222",
    "223",
    "224",
    "225",
    "226",
    "227",
    "228",
    "229",
    "230",
    "231",
    "232",
    "233",
    "234",
    "235",
    "236",
    "237",
    "238",
    "239",
    "240",
    "241",
    "242",
    "243",
    "244",
    "245",
    "246",
    "248",
    "249",
    "250",
    "251",
    "252",
    "253",
    "254",
    "255",
    "256",
    "257",
    "258",
    "260",
    "261",
    "262",
    "263",
    "264",
    "265",
    "266",
    "267",
    "268",
    "269",
    "27",
    "290",
    "291",
    "297",
    "298",
    "299",
    "30",
    "31",
    "32",
    "33",
    "34",
    "350",
    "351",
    "352",
    "353",
    "354",
    "355",
    "356",
    "357",
    "358",
    "359",
    "36",
    "370",
    "371",
    "372",
    "373",
    "374",
    "375",
    "376",
    "377",
    "378",
    "380",
    "381",
    "382",
    "383",
    "385",
    "386",
    "387",
    "389",
    "39",
    "40",
    "41",
    "420",
    "421",
    "423",
    "43",
    "44",
    "45",
    "46",
    "47",
    "48",
    "49",
    "500",
    "501",
    "502",
    "503",
    "504",
    "505",
    "506",
    "507",
    "508",
    "509",
    "51",
    "52",
    "53",
    "54",
    "55",
    "56",
    "57",
    "58",
    "590",
    "591",
    "592",
    "593",
    "594",
    "595",
    "596",
    "597",
    "598",
    "599",
    "60",
    "61",
    "62",
    "63",
    "64",
    "65",
    "66",
    "670",
    "672",
    "673",
    "674",
    "675",
    "676",
    "677",
    "678",
    "679",
    "680",
    "681",
    "682",
    "683",
    "684",
    "685",
    "686",
    "687",
    "688",
    "689",
    "690",
    "691",
    "692",
    "7",
    "77",
    "81",
    "82",
    "84",
    "850",
    "852",
    "853",
    "855",
    "856",
    "86",
    "870",
    "880",
    "881",
    "882",
    "883",
    "883130",
    "886",
    "90",
    "91",
    "92",
    "93",
    "94",
    "95",
    "960",
    "961",
    "962",
    "963",
    "964",
    "965",
    "966",
    "967",
    "968",
    "970",
    "971",
    "972",
    "973",
    "974",
    "975",
    "976",
    "977",
    "98",
    "992",
    "993",
    "994",
    "995",
    "996",
    "998",
]

SOURCES = [
    "Life_mnp",
    "directory",
    "jtnav",
    "infobip",
    "cmtelecom",
    "infobip_hlr",
    "infobip_ltcy2",
    "infobip_2",
    "mitto_hlr",
    "mitto_mnp",
    "svyazcom_sure",
    "svyazcom_bics",
    "svyazcom_bics_346",
    "svyazcom_comfone",
    "svyazcom_comfone_134",
    "svyazcom_coolmessages",
    "tyntec_mnp",
    "horisen_mnp",
    "tyntec_hlr",
    "hlr_tyntec_e140",
    "tmt_mnp",
    "tmt_hlr",
    "mnp",
    "upstream",
    "mnp_kazinfotech",
    "messagebird",
    "xconnect_mnp",
    "mediafon_bkp",
    "mediafon_mnp",
    "kaleyra",
    "infinite_mnp",
    "netnumber_mnp",
    "netnumber_hlr",
    "refbook",
]


def arg_parse():
    parser = argparse.ArgumentParser(description="parse raw hlr log")
    subparser = parser.add_subparsers(help="choose command")

    summary_parser = subparser.add_parser(
        "summary",
        help="return summary for provided log file",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    summary_parser.set_defaults(callback=summary_command)

    converter_parser = subparser.add_parser(
        "convert",
        help="convert raw log to csv format",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    converter_parser.set_defaults(callback=converter_command)

    # common argument for subparser
    for sub_parser in [summary_parser, converter_parser]:
        sub_parser.add_argument("-f", "--file", help="path to log file")
    parser.add_argument(
        "--sources-list",
        required=False,
        action="store_true",
        help="return list of sources",
    )
    return parser.parse_args()


def parse_log(file_in: str) -> Dict[str, Dict[str, Dict]]:
    resp: Dict[str, Dict[str, Dict]] = {source: {} for source in SOURCES}
    request_not_found = 0
    unknown_source = {}
    with open(file_in, "r") as f:
        for line in f:
            resp = parse_line(line, request_not_found, resp, unknown_source)
    return resp


def parse_line(line: str,
               request_not_found: int,
               resp: Dict[str, Dict[str, Dict]],
               unknown_source: Dict[str, int]):
    if "request started" in line:
        start_req_format = r"^(.*): request started. Request ID: (\d*).*DNIS: (\d*); source name: (\w*)"
        start_time, req_id, msisdn, source = re.search(start_req_format, line).groups()
        try:
            resp[source][req_id] = {
                "start_time": start_time,
                "msisdn": msisdn,
                "mccmnc": None,
                "result": None,
                "proc_time": None,
                "ported": None,
                "cached": None,
                "message": None,
            }
        except KeyError:
            if source in unknown_source.keys():
                unknown_source[source] += 1
            else:
                unknown_source[source] = 1
    if "request ended" in line:
        end_resp_format = (r".*Request ID: (\d*);.*MCCMNC: (\d{0,6}); "
                           r"result: (.*); proctime: (\d*\.\d*);.*"
                           r"ported: (\d*); cached: (\d*); source name: (\w*);.* message: (.*)")
        search = re.search(
            end_resp_format,
            line,
        )
        (
            req_id,
            mccmnc,
            result,
            proc_time,
            ported,
            cached,
            source,
            message,
        ) = search.groups()
        try:
            resp[source][req_id]["mccmnc"] = mccmnc
            resp[source][req_id]["result"] = result
            resp[source][req_id]["proc_time"] = proc_time
            resp[source][req_id]["ported"] = ported
            resp[source][req_id]["cached"] = cached
            resp[source][req_id]["message"] = message
        except KeyError:
            request_not_found += 1
    return resp


def write_log(file_out, parsed_log):
    with open(file_out, "w") as f:
        csv_writer = csv.writer(f, delimiter=";")
        rows = [
            "start_time",
            "msisdn",
            "mccmnc",
            "result",
            "proc_time",
            "ported",
            "cached",
            "message",
        ]
        csv_writer.writerow(rows)
        for req_id in parsed_log.keys():
            csv_writer.writerow(
                [
                    parsed_log[req_id].get("start_time"),
                    parsed_log[req_id].get("msisdn"),
                    parsed_log[req_id].get("mccmnc"),
                    parsed_log[req_id].get("result"),
                    parsed_log[req_id].get("proc_time"),
                    parsed_log[req_id].get("ported"),
                    parsed_log[req_id].get("cached"),
                    parsed_log[req_id].get("message"),
                ]
            )


def summary_command(arguments):
    parsed_log = parse_log(file_in=arguments.file)
    calculate_summary(parsed_log)


def converter_command(arguments):
    parsed_log = parse_log(arguments.file)
    file_in = pathlib.Path(arguments.file)
    file_out = pathlib.Path.joinpath(
        file_in.parent, f"{pathlib.Path(arguments.file).stem}.csv"
    )
    write_log(file_out, parsed_log)
    print(f"log saved to {file_out}")


def calculate_summary(parsed_log: Dict[str, Dict[str, Dict]]):
    for source, requests in parsed_log.items():
        request_cnt = len(requests)
        if request_cnt:
            resp_details = [value for value in requests.values()]
            failed_requests = list(filter(lambda x: x["result"] == "-1", resp_details))
            failed_request_count = len(failed_requests)
            failed_by_country = group_failed_requests(failed_requests)
            cached_request = len(list(filter(lambda x: x["cached"] == "1", resp_details)))
            min_proc_time = min(
                [
                    float(detail.get("proc_time"))
                    for detail in resp_details
                    if detail.get("proc_time") is not None and detail.get("cached") == "0"
                ]
            )
            max_proc_time = max(
                [
                    float(detail.get("proc_time"))
                    for detail in resp_details
                    if detail.get("proc_time") is not None
                ]
            )
            without_resp = list(filter(lambda x: x["result"] is None, resp_details))
            requests_without_response_detail = [
                (resp.get("start_time"), resp.get("msisdn")) for resp in without_resp
            ]
            resp_time_th1 = 0
            resp_time_th2 = 0
            resp_time_th3 = 0
            for parsed_log in resp_details:
                if parsed_log["cached"] == "0" and parsed_log["result"]:
                    if float(parsed_log["proc_time"]) <= 7:
                        resp_time_th1 += 1
                    elif 7 < float(parsed_log["proc_time"]) <= 10:
                        resp_time_th2 += 1
                    else:
                        resp_time_th3 += 1
            print(source)
            print('=' * 100)
            print(f"{request_cnt=}")
            print(f"{failed_request_count=}")
            print(f"{failed_by_country=}")
            print(f"{cached_request=}")
            print(f"{min_proc_time=}")
            print(f"{max_proc_time=}")
            print(f"requests_without_response={len(without_resp)}")
            print(f"{requests_without_response_detail=}")
            print(f"{resp_time_th1=}")
            print(f"{resp_time_th2=}")
            print(f"{resp_time_th3=}")
            print('=' * 100)


def group_failed_requests(failed_requests: List[Dict[str, str]]) -> Dict[str, int]:
    failed_by_country = {}
    for failed_request in failed_requests:
        matched_cc = []
        for cc in COUNTRY_CODES:
            if failed_request["msisdn"].startswith(cc):
                matched_cc.append(cc)
        longest_cc = max(matched_cc)
        if longest_cc not in failed_by_country:
            failed_by_country[longest_cc] = 1
        else:
            failed_by_country[longest_cc] += 1
    return failed_by_country


def main():
    args = arg_parse()
    args.callback(args)


if __name__ == "__main__":
    main()
