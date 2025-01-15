"""
Сбор и обработка проектных метрик из Youtrack
"""
import sys
import os
import json
import logging
import requests
import numpy as np
import datetime

import db

from requests.exceptions import HTTPError

percent_list = dict()
throughputList = []

LT_PRJ1_WIP = '33-8074'  # It's "PRJ1 LeadTime WIP" saved search - put ID of you PRJ1 project
LT_PRJ1_QUERY_NUM = "33-5572"  # It's "LeadTime monitoring" saved search
LT_PRJ2_QUERY_NUM = "33-6895"  # It's "PRJ2 LeadTime monitoring" saved search
LT_PRJ3_QUERY_NUM = "33-7238"  # It's "PRJ3 LeadTime monitoring" saved search
TESTING_DELAY_QUERY_NUM = "33-7118"  # It's "Testing delay" saved search
AUTO_LAG_QUERY_NUM = '33-7141'  # It's "Auto lag" saved search

parsed_list = []
qa_testing_delay = []
qa_auto_lag = []
wip_list = []

# The following teams should not take part in the analysis, ignore them
teams_ignore_list = {
    'Analysts',
    'DevOps',
    'Operational',
    'QA',
    'TW',
    'FE'}

prj1_teams = ['Team1',
              'Team2',
              'Team3',
              'Team4',
              'Team5']

prj2_teams = ['Team6']

prj3_teams = ['Team7']

teams = ['Team1',
         'Team2',
         'Team3',
         'Team4',
         'Team5',
         'Team6',
         'Team7']

task_types = [
    'Feature',
    'Story',
    'Enabler',
    'PoC',
    'Bug']

team_agile_board = {
    "Team1 Sprints": "Team1",
    "Team2 Sprints": "Team2",
    "Team3 Sprints": "Team3",
    "Team4 Sprints": "Team4",
    "Team5 Sprints": "Team5",
    "Team6 Sprints": "Team6",
    "Team7 Sprints": "Team7"
}

youtrack_token = ""


def init_logging():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s: %(message)s', datefmt='%m/%d/%Y %H:%M:%S',
                        stream=sys.stdout)


def get_youtrack_token():
    global youtrack_token

    logging.info("get_youtrack_token")
    if os.environ.get("YOUTRACK_TOKEN"):
        youtrack_token = os.environ["YOUTRACK_TOKEN"]
    else:
        logging.error("YOUTRACK_TOKEN must be defined as an environment variable!")
        print("ERROR: YOUTRACK_TOKEN must be defined as an environment variable!")
        exit(1)


def get_youtrack(path, fields):
    """
    Handle GET request to Youtrack

    Args:
        path   - main API path for query,
                 Take into account that root path (https://youtrack.ptsecurity.com/api) SHOULD NOT be included!
        fields - a set of fields to be requested

    Returns:
        A tuple containing:
            tuple[0] - result (True/False) of GET request execution
            tuple[1] - JSON containing response from Youtrack
    """
    youtrack_root = "https://youtrack.ptsecurity.com/api/"
    youtrack_request_headers = {"Accept": "application/json",
                                "Authorization": "Bearer perm:" + youtrack_token,
                                "Content-Type": "application/json"}

    data = []
    response = {}
    result = True

    url = youtrack_root + path
    try:
        response = requests.get(url, headers=youtrack_request_headers, params=fields)
        response.raise_for_status()
    except HTTPError as e:
        logging.error("Error during access to Youtrack")
        logging.error(f"Error code: {response.status_code}")
        logging.error(f"Error description: {response.reason}")
        print("Error during access to Youtrack")
        print("Error code: ", response.status_code)
        print("Error description: ", response.reason)
        result = False

    # print("DEBUG: Success")
    # print("DEBUG: Result:")
    # print(json.dumps(response.json(), indent=2))
    return result, response.json()


def get_saved_search(id, stream_name):
    """
    Get result of saved search
    :return:
    """
    logging.info(f"get_saved_search for {stream_name} stream")

    url = "savedQueries/" + id
    fields = "fields=id,name,issues(id,idReadable,created,resolved,summary,customFields(id,name,value(id,name,value)))"
    data = get_youtrack(url, fields)
    issues_list = ""
    if data[0]:
        issues_list = data[1].get("issues")

    debug_list = json.dumps(issues_list)
    return issues_list


def get_prj1_sprints():
    logging.info("get_PRJ1_sprints")
    prj1_project = "69-239"  # id of PRJ1 project in Youtrack
    sprint_planned_id = "70-2021"  # id of field "Sprint planned" in PRJ1 project
    url = "admin/projects/" + prj1_project + "/customFields/" + sprint_planned_id
    fields = "fields=id,bundle(id,name,values(id,name,archived,startDate,releaseDate))"
    data = get_youtrack(url, fields)

    parsed_sprint_list = []

    if data[0]:
        sprint_list = data[1]["bundle"]["values"]
        for sprint in sprint_list:
            parsed_sprint = dict()
            sprint_start = sprint["startDate"]
            sprint_finish = sprint["releaseDate"]
            if sprint_finish and sprint_start:
                releaseDate = datetime.datetime.fromtimestamp(int(sprint["releaseDate"]) / 1000)
                startDate = datetime.datetime.fromtimestamp(int(sprint["startDate"]) / 1000)
                parsed_sprint["name"] = sprint["name"]
                parsed_sprint["isArchived"] = sprint["archived"]
                parsed_sprint["startDate"] = startDate.date()
                parsed_sprint["releaseDate"] = releaseDate.date()
                parsed_sprint_list.append(parsed_sprint)
    return parsed_sprint_list


def get_agile_boards():
    top = 30
    skip = 0
    next_skip = 30
    iteration = 0
    more_page = True
    target_agiles = dict()

    logging.info("get_agile_boards")
    while more_page:
        url = "agiles"
        fields = f"fields=id,name,owner(id,name),projects(id,name),sprints(id,name,archived)&$top={top}&$skip={skip}"
        data = get_youtrack(url, fields)
        if data[0]:
            iteration = iteration + 1
            skip = next_skip * iteration
            agiles_list = data[1]
            if agiles_list:
                iteration = iteration + 1
                # logging.debug(f"Start handling response. Cycle: {iteration}")
                for agile in agiles_list:
                    agile_name = agile["name"]
                    # logging.debug(f"Agile name: {agile_name}")
                    if agile_name in team_agile_board.keys():
                        target_sprints = []
                        for sprint in agile["sprints"]:
                            if not sprint["archived"]:
                                target_sprints.append(sprint)
                        agile_attr = {"id": agile["id"], "name": agile_name, "sprints": target_sprints}
                        team_name = team_agile_board[agile_name]
                        target_agiles[team_name] = agile_attr
            else:
                more_page = False
    # logging.debug(f"Agiles: {target_agiles}")
    return target_agiles


def handle_agiles(agiles):
    logging.info("handle_agiles")
    for team, agile_values in agiles.items():
        agile_id = agile_values["id"]
        for sprint in agile_values["sprints"]:
            sprint_name = sprint["name"]
            sprint_id = sprint["id"]
            # logging.info(f"Get issues for team {team}. Sprint: {sprint_name}")
            # get issues for each sprint
            url = f"agiles/{agile_id}/sprints/{sprint_id}"
            fields = "fields=id,archived,start,finish,goal,issues(id,idReadable,customFields(id,name,value(id,name,value))),isDefault,name,previousSprint(finish,name,id),unresolvedIssuesCount"
            data = get_youtrack(url, fields)
            if data[0]:
                sprint_info = data[1]
                issues_total_num = 0
                story_total_num = 0
                story_completed_num = 0
                enabler_total_num = 0
                enabler_completed_num = 0
                poc_total_num = 0
                poc_completed_num = 0
                bug_total_num = 0
                bug_completed_num = 0
                for issue in sprint_info["issues"]:
                    issue_type = ""
                    issue_state = ""
                    for custom_field in issue["customFields"]:
                        if custom_field["name"] == "Type":
                            issue_type = custom_field["value"]["name"]
                        elif custom_field["name"] == "State":
                            issue_state = custom_field["value"]["name"]
                    if issue_type == "Story":
                        issues_total_num = issues_total_num + 1
                        story_total_num = story_total_num + 1
                        if issue_state == "Completed":
                            story_completed_num = story_completed_num + 1
                    elif issue_type == "Enabler":
                        issues_total_num = issues_total_num + 1
                        enabler_total_num = enabler_total_num + 1
                        if issue_state == "Completed":
                            enabler_completed_num = enabler_completed_num + 1
                    elif issue_type == "PoC":
                        issues_total_num = issues_total_num + 1
                        poc_total_num = poc_total_num + 1
                        if issue_state == "Completed":
                            poc_completed_num = poc_completed_num + 1
                    elif issue_type == "Bug":
                        issues_total_num = issues_total_num + 1
                        bug_total_num = bug_total_num + 1
                        if issue_state == "Completed":
                            bug_completed_num = bug_completed_num + 1
                # print(f"DEBUG:      Story:   {story_completed_num}/{story_total_num}")
                # print(f"DEBUG:      Enabler: {enabler_completed_num}/{enabler_total_num}")
                # print(f"DEBUG:      PoC:     {poc_completed_num}/{poc_total_num}")
                # print(f"DEBUG:      Bug:     {bug_completed_num}/{bug_total_num}")
                # print(f"DEBUG:      issues_total_num = {issues_total_num}")
                # completed_num = story_completed_num + enabler_completed_num + poc_completed_num + bug_completed_num
                # calc_uncompleted = issues_total_num - completed_num
                # print(f"DEBUG:      calc_uncompleted = {calc_uncompleted}")
                attr = dict()
                attr["team"] = team
                attr["sprint"] = sprint
                attr["bugTotal"] = bug_total_num
                attr["bugCompleted"] = bug_completed_num
                attr["storyTotal"] = story_total_num
                attr["storyCompleted"] = story_completed_num
                attr["pocTotal"] = poc_total_num
                attr["pocCompleted"] = poc_completed_num
                attr["enablerTotal"] = enabler_total_num
                attr["enablerCompleted"] = enabler_completed_num
                throughputList.append(attr)


def parse_wip_list(wip_issues):
    logging.info("parse_wip_list")

    for issue in wip_issues:
        ignore_row = False
        id = issue["idReadable"]
        attr = {"id": id, "summary": issue["summary"]}
        for field in issue["customFields"]:
            if field["name"] == "Type":
                attr["type"] = field["value"]["name"]
            elif field["name"] == "State":
                attr["state"] = field["value"]["name"]
            elif field["name"] == "Team":
                if field["value"] is None:
                    attr["team"] = "None"
                    # ignore_row = True
                    logging.error(f"No team set for ID: {id}")
                else:
                    attr["team"] = field["value"]["name"]
                    if attr["team"] in teams_ignore_list:
                        ignore_row = True
        if not ignore_row:
            wip_list.append(attr)


def parse_product_metrics(stream_issues, sprint_list):
    logging.info("parse_issues_list_json")

    for stream, issues_list in stream_issues.items():
        for issue in issues_list:
            ignore_row = False
            attr = {"stream": stream, "id": issue["idReadable"], "summary": issue["summary"], "sprint": "None"}
            id = attr["id"]
            if stream == "PRJ3":  # FIXME: It's because PRJ3 has no 'Team' field
                attr["team"] = "PRJ3"
            for field in issue["customFields"]:
                if field["name"] == "Type":
                    attr["type"] = field["value"]["name"]
                elif field["name"] == "State":
                    attr["state"] = field["value"]["name"]
                elif field["name"] == "Team":
                    if field["value"] is None:
                        attr["team"] = "None"
                        # ignore_row = True
                        logging.error(f"No team set for ID: {id}")
                    else:
                        if stream == 'PRJ2':  # FIXME: It's WA because PRJ2 has 'Team' multiple selection
                            attr["team"] = "PRJ2"
                        else:
                            attr["team"] = field["value"]["name"]
                            if attr["team"] in teams_ignore_list:
                                ignore_row = True
                elif field["name"] == "Start date":
                    attr["startDate"] = field["value"]
                elif field["name"] == "Completed_time":
                    if stream == 'PRJ2' or stream == 'PRJ3':
                        attr["completedTime"] = issue["resolved"]
                    else:
                        attr["completedTime"] = field["value"]
                    millisecondsCompletedTime = attr["completedTime"]
                    if millisecondsCompletedTime:
                        completedDate = (datetime.datetime.fromtimestamp(int(millisecondsCompletedTime) / 1000)).date()
                        id = attr["id"]
                        for sprint in sprint_list:
                            if (completedDate > sprint["startDate"]) and (completedDate < sprint["releaseDate"]):
                                attr["sprint"] = sprint["name"]
                        sprint_name = attr.get("name", None)
                elif field["name"] == "LeadTime":
                    attr["leadTime"] = field["value"]
                    if attr["leadTime"] is None:
                        ignore_row = True
                        if attr["team"] not in teams_ignore_list:
                            logging.error(f"No leadtime for id: {id}")
            if not ignore_row:
                parsed_list.append(attr)


def parse_testing_delay(issues_list):
    logging.info("parse_testing_delay")

    error_value = 2147483647
    ignore_row = False
    for issue in issues_list:
        attr = {"id": issue["idReadable"], "summary": issue["summary"]}
        resolved = issue["resolved"]
        attr["resolved_ms"] = resolved
        attr["resolved"] = datetime.datetime.fromtimestamp(int(resolved) / 1000)
        attr["quarter"] = get_quarter(resolved)
        for field in issue["customFields"]:
            if field["name"] == "RFT_period":
                attr["rft_period"] = field["value"]
            elif field["name"] == "Testing_period":
                attr["testing_period"] = field["value"]
        if (attr["rft_period"] is None) or (attr["testing_period"] is None):
            ignore_row = True
            id = attr["id"]
            logging.error(f"Issue {id} has 'Testing period' or 'RFT_period' equal 0 or null")
        if (attr["rft_period"] == error_value) or (attr["testing_period"] == error_value):
            ignore_row = True
            id = attr["id"]
            logging.error(f"Issue {id} has 'Testing period' or 'RFT_period' equal to {error_value}")
        if not ignore_row:
            qa_testing_delay.append(attr)


def parse_auto_delay(issues_list):
    logging.info("parse_auto_delay")

    ignore_row = False
    for issue in issues_list:
        attr = {"id": issue["idReadable"], "summary": issue["summary"]}
        resolved = issue["resolved"]
        attr["resolved"] = datetime.datetime.fromtimestamp(int(resolved) / 1000)
        attr["quarter"] = get_quarter(resolved)
        for field in issue["customFields"]:
            if field["name"] == "Autotest lag":
                attr["auto_lag"] = field["value"]
                if (attr["auto_lag"] == 0) or (attr["auto_lag"] is None):
                    ignore_row = True
                    id = attr["id"]
                    logging.error(f"Issue {id} has 'Autotest lag' equal 0 or null")
        if not ignore_row:
            qa_auto_lag.append(attr)


def percentile_per_type(array):
    plist = {"10": 0, "25": 0, "50": 0, "75": 0, "90": 0, "Count": 0}
    if len(array) > 0:
        # print(f"DEBUG: array: {str(array)}")
        val_list = np.array(array)
        plist["10"] = np.percentile(val_list, 10)
        plist["25"] = np.percentile(val_list, 25)
        plist["50"] = np.percentile(val_list, 50)
        plist["75"] = np.percentile(val_list, 75)
        plist["90"] = np.percentile(val_list, 90)
        plist["Count"] = len(array)
    return plist


def get_quarter(completed_time):
    # logging.debug(f"completed_time: {completed_time}")
    target_quarter = ""
    year = datetime.date.today().year

    q1_quarter = datetime.datetime.strptime('31.03.' + str(year), '%d.%m.%Y')
    q2_quarter = datetime.datetime.strptime('30.06.' + str(year), '%d.%m.%Y')
    q3_quarter = datetime.datetime.strptime('30.09.' + str(year), '%d.%m.%Y')
    q4_quarter = datetime.datetime.strptime('31.12.' + str(year), '%d.%m.%Y')

    quarter = datetime.datetime.fromtimestamp(int(completed_time) / 1000)
    if quarter < q1_quarter:
        target_quarter = "Q1"
    elif quarter < q2_quarter:
        target_quarter = "Q2"
    elif quarter < q3_quarter:
        target_quarter = "Q3"
    elif quarter < q4_quarter:
        target_quarter = "Q4"

    return target_quarter


def init_perc_container(container):
    for team in teams:
        container[team] = dict()
        for type in task_types:
            container[team][type] = dict()
            for quarter in ["Q1", "Q2", "Q3", "Q4"]:
                container[team][type][quarter] = []


def calc_percentiles():
    logging.info("calc_percentiles")
    leadtimes = dict()
    percentiles = dict()
    init_perc_container(leadtimes)
    init_perc_container(percentiles)
    for row in parsed_list:
        id = row["id"]
        stream = row["stream"]
        leadTime = row["leadTime"]
        taskType = row["type"]
        completedTime = row['completedTime']
        team = row["team"]
        if leadTime == "None":
            logging.error(f"ERROR: calc_percentiles: No leadtime for id: {id}")

        quarter = get_quarter(completedTime)
        if team in teams:
            leadtimes[team][taskType][quarter].append(leadTime)
            # logging.debug(f"Leadtimes: {leadtimes}")

    for team, team_values in leadtimes.items():
        for types, type_values in team_values.items():
            type_total_list = []
            for quarter, quarter_values in type_values.items():
                type_total_list = type_total_list + quarter_values
                percentiles[team][types][quarter] = percentile_per_type(quarter_values)
            percentiles[team][types]["Total"] = percentile_per_type(type_total_list)

    # logging.debug(f"Percentiles: {str(percentiles)}")
    return percentiles


def calc_distribution():
    logging.info("calc_distribution")

    my_list = []
    my_dict = dict()
    for item in parsed_list:
        attr = {}
        stream = item["stream"]
        id = item["id"]
        type = item["type"]
        sprint = item["sprint"]
        team = item["team"]
        leadtime = item["leadTime"]

        attr["stream"] = stream
        attr["id"] = id
        attr["type"] = type
        attr["sprint"] = sprint
        attr["team"] = team
        attr["leadtime"] = leadtime

        my_list.append(attr)


def create_tables_in_db_if_not_exist():
    logging.info("create_tables_in_db_if_not_exist")

    db.exec('CREATE TABLE IF NOT exists public.percentile ( '
            'id varchar(30) NOT NULL, '
            'team varchar(15) NULL, '
            'quarter varchar(5) NULL,'
            'type varchar(15) NULL, '
            'p10 float4 NULL, '
            'p25 float4 NULL, '
            'p50 float4 NULL, '
            'p75 float4 NULL, '
            'p90 float4 NULL, '
            'count float4 NULL, '
            'stream varchar(30) NULL,'
            'CONSTRAINT percentile_pk PRIMARY KEY (id) );')

    db.exec('CREATE TABLE IF NOT exists public.leadtime ( '
            'stream varchar(30) NULL,'
            'id varchar(15) NOT NULL,'
            '"type" varchar(15) NULL,'
            'state varchar(30) NULL,'
            'summary text NULL,'
            'team varchar(20) NULL,'
            'startdate int8 NULL,'
            'completedtime int8 NULL,'
            'leadtime int2 NULL,'
            'CONSTRAINT leadtime_id_key UNIQUE (id),'
            'CONSTRAINT leadtime_pk PRIMARY KEY (id));')

    db.exec('CREATE TABLE IF NOT exists public.throughput ( '
            'id varchar(70) NOT NULL,'
            'team varchar(30) NULL,'
            'sprint varchar(50) NULL,'
            'bugtotal int2 NULL,'
            'bugcompleted int2 NULL,'
            'storytotal int2 NULL,'
            'storycompleted int2 NULL,'
            'poctotal int2 NULL,'
            'poccompleted int2 NULL,'
            'enablertotal int2 NULL,'
            'enablercompleted int2 NULL,'
            'CONSTRAINT throughput_pk PRIMARY KEY (id),'
            'CONSTRAINT throughput_unique UNIQUE (id));')

    db.exec('CREATE TABLE IF NOT exists public.testing_delay ('
            'id varchar(30) NOT NULL,'
            'summary text NULL,'
            'rft_period int8 NULL,'
            'testing_period int8 NULL,'
            'resolved timestamp NULL,'
            'quarter varchar(2) NULL,'
            'CONSTRAINT testing_delay_pk PRIMARY KEY (id));')

    db.exec('CREATE TABLE IF NOT exists public.auto_delay ('
            'id varchar(15) NOT NULL,'
            'summary text NULL,'
            'auto_lag int8 NULL,'
            'resolved timestamp NULL,'
            'quarter varchar(2) NULL,'
            'CONSTRAINT auto_delay_pk PRIMARY KEY (id));')

    db.exec('CREATE TABLE IF NOT exists public.wip_list ('
            'id varchar(15) NULL,'
            '"type" varchar(15) NULL,'
            'state varchar(30) NULL,'
            'summary text NULL,'
            'team varchar(20) NULL,'
            'CONSTRAINT wip_list_unique UNIQUE (id),'
            'CONSTRAINT wip_list_pk PRIMARY KEY (id));')

    db.exec('CREATE TABLE IF NOT exists public.leadtime_distribution ('
            'id int4 NULL,'
            'count int4 NULL'
            ');')


def load_leadtimedb():
    logging.info("load_leadtimedb")

    leadtime_list = []
    lt_records = db.get("SELECT id FROM leadtime")
    for row in lt_records:
        leadtime_list.append(row[0])

    return leadtime_list


def save_issues_in_db(lt_id_list):
    logging.info("save_issues_in_db")
    for issue in parsed_list:
        issue_id = issue["id"]
        if issue_id in lt_id_list:
            # Update ALL fields in leadtime
            db.exec("UPDATE leadtime SET stream = %s, state = %s, summary = %s, team = %s, startdate = %s, "
                    "completedtime = %s, leadtime = %s, sprint = %s where id = %s",
                    (issue["stream"], issue["state"], issue["summary"],
                     issue["team"], issue["startDate"], issue["completedTime"],
                     issue["leadTime"], issue["sprint"], issue["id"]))
        else:
            db.exec("INSERT INTO leadtime (stream, id, type, state, summary, "
                    "team, startdate,"
                    "completedtime, leadtime, sprint) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (ID) DO UPDATE "
                    "SET stream=EXCLUDED.stream, startdate=EXCLUDED.startdate, "
                    "completedtime=EXCLUDED.completedtime, "
                    "leadtime=EXCLUDED.leadtime",
                    (issue["stream"], issue["id"], issue["type"], issue["state"], issue["summary"],
                     issue["team"], issue["startDate"], issue["completedTime"], issue["leadTime"], issue["sprint"]))

    for issue in wip_list:
        db.exec("INSERT INTO wip_list (id, type, state, summary, team) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (ID) DO UPDATE "
                "SET type=EXCLUDED.type, state=EXCLUDED.state, summary=EXCLUDED.summary, team=EXCLUDED.team",
                (issue["id"], issue["type"], issue["state"], issue["summary"], issue["team"]))


def save_test_delay_in_db():
    logging.info("save_test_delay_in_db")

    for issue in qa_testing_delay:
        db.exec("INSERT INTO testing_delay (id, summary, rft_period, testing_period,"
                "resolved, resolved_ms, quarter) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (id) DO UPDATE "
                "SET summary = EXCLUDED.summary, rft_period=EXCLUDED.rft_period, "
                "testing_period=EXCLUDED.testing_period, resolved = EXCLUDED.resolved,"
                "resolved_ms = EXCLUDED.resolved_ms, quarter = EXCLUDED.quarter",
                (issue["id"], issue["summary"], issue["rft_period"], issue["testing_period"],
                 issue["resolved"], issue["resolved_ms"], issue["quarter"]))


def save_auto_lag_in_db():
    logging.info("save_auto_lag_in_db")

    for issue in qa_auto_lag:
        row = []
        db.exec("INSERT INTO auto_delay (id, summary, auto_lag, resolved, quarter) "
                "VALUES (%s, %s, %s, %s, %s) "
                "ON CONFLICT (id) DO UPDATE "
                "SET summary = EXCLUDED.summary, auto_lag=EXCLUDED.auto_lag, "
                "resolved = EXCLUDED.resolved, quarter = EXCLUDED.quarter",
                (issue["id"], issue["summary"], issue["auto_lag"], issue["resolved"], issue["quarter"]))


def save_distribution_to_db(fd_dict):
    logging.info("save_distribution_to_db")

    db.exec("DELETE from leadtime_distribution")
    for item in fd_dict:
        id = item[0]
        count = item[1]
        db.exec("INSERT INTO leadtime_distribution (id, count) "
                "VALUES (%s, %s)", (id, count))


def save_throughput_to_db():
    logging.info("save_throughput_to_db")

    for item in throughputList:
        sprint_name = item["sprint"]["name"]
        id = str(item["team"]) + "_" + sprint_name
        db.exec("INSERT INTO public.throughput (id, team, sprint,"
                "bugtotal, bugcompleted,"
                "storytotal, storycompleted,"
                "poctotal, poccompleted,"
                "enablertotal, enablercompleted) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (id) DO UPDATE "
                "SET team=EXCLUDED.team, sprint=EXCLUDED.sprint, "
                "bugtotal=EXCLUDED.bugtotal, bugcompleted=EXCLUDED.bugcompleted, "
                "storytotal=EXCLUDED.storytotal, storycompleted=EXCLUDED.storycompleted, "
                "poctotal=EXCLUDED.poctotal, poccompleted=EXCLUDED.poccompleted, "
                "enablertotal=EXCLUDED.enablertotal, enablercompleted=EXCLUDED.enablercompleted",
                (id, item["team"], sprint_name, item["bugTotal"], item["bugCompleted"],
                 item["storyTotal"], item["storyCompleted"],
                 item["pocTotal"], item["pocCompleted"],
                 item["enablerTotal"], item["enablerCompleted"],))


def save_percentiles_to_db(result):
    logging.info("save_percentiles_to_db")

    for team, team_values in result.items():  # key: team, value: quarter
        for task_type, type_values in team_values.items():
            for quarter, quarter_values in type_values.items():
                id = str(team) + "_" + str(task_type) + "_" + str(quarter)
                stream = ''
                if team in prj1_teams:
                    stream = 'PRJ1'
                elif team in prj2_teams:
                    stream = 'PRJ2'
                elif team in prj3_teams:
                    stream = 'PRJ3'
                else:
                    logging.error(f"save_percentiles_to_db: UNKNOWN team:{team}")
                # print(f"team: {team}; type: {task_type}; quarter: {quarter}; values: {str(quarter_values)}")
                db.exec("INSERT into public.percentile (id, team, quarter, type,"
                        "p10, p25, p50, p75, p90, count, stream) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT (ID) DO UPDATE "
                        "SET team=EXCLUDED.team, quarter=EXCLUDED.quarter, type=EXCLUDED.type,"
                        "p10=EXCLUDED.p10, p25=EXCLUDED.p25, p50=EXCLUDED.p50, p75=EXCLUDED.p75,"
                        "p90=EXCLUDED.p90, count=EXCLUDED.count, stream=EXCLUDED.stream",
                        (id, team, quarter, task_type,
                         str(quarter_values["10"]), str(quarter_values["25"]),
                         str(quarter_values["50"]), str(quarter_values["75"]),
                         str(quarter_values["90"]), str(quarter_values["Count"]), stream))


if __name__ == '__main__':
    db = db.Database()
    init_logging()
    logging.info("Start...")

    create_tables_in_db_if_not_exist()
    get_youtrack_token()

    lt_id_list = load_leadtimedb()

    stream_issues = {"PRJ1": {}, "PRJ2": {}, "PRJ3": {}}
    # Достаем и обрабатываем Leadtime
    wip_issues = get_saved_search(LT_PRJ1_WIP, "PRJ1")
    stream_issues["PRJ1"] = get_saved_search(LT_PRJ1_QUERY_NUM, "PRJ1")
    stream_issues["PRJ2"] = get_saved_search(LT_PRJ2_QUERY_NUM, "PRJ2")
    stream_issues["PRJ3"] = get_saved_search(LT_PRJ3_QUERY_NUM, "PRJ3")
    qa_test_delay = get_saved_search(TESTING_DELAY_QUERY_NUM, "QA Testing delay")
    qa_test_lag = get_saved_search(AUTO_LAG_QUERY_NUM, "QA Autotests delay")

    sprint_list = get_prj1_sprints()

    parse_product_metrics(stream_issues, sprint_list)
    parse_testing_delay(qa_test_delay)
    parse_auto_delay(qa_test_lag)
    parse_wip_list(wip_issues)
    save_issues_in_db(lt_id_list)
    save_test_delay_in_db()
    save_auto_lag_in_db()
    #
    # Считаем перцентили
    result = calc_percentiles()
    save_percentiles_to_db(result)

    distr_list = calc_distribution()
    save_distribution_to_db(distr_list)

    # Достаем и считаем Throughput
    agiles = get_agile_boards()
    handle_agiles(agiles)
    save_throughput_to_db()
    logging.info("Finish...")
    sys.exit(0)
