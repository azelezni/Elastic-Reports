from elasticsearch import exceptions as es_exceptions
from elasticsearch import Elasticsearch
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functools import reduce

import smtplib
import operator
import logging
import yaml
import os

# Read config file
try:
    logging.info('Reading config file at {0}/conf.yml'.format(os.getcwd()))
    with open('./conf.yml', 'r') as infile:
        conf = yaml.load(infile)
except FileNotFoundError:
    logging.error('Configuration file not found.')
    exit(1)

# Configure logger
logger = logging.getLogger('elastic_reports')
hdlr = logging.FileHandler(conf['log_file'])
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.getLevelName(conf['log_level'].upper()))


def es_instance(credentials):
    """
    Connect to Elasticsearch
    :return: 
    """
    if all(x in credentials for x in ['username', 'password']):
        logger.info('Connecting to ES instance at {0}@{1}:{2}'.format(
            credentials['username'],
            conf['elasticsearch']['address'],
            conf['elasticsearch']['port']
        ))
        es = Elasticsearch(conf['elasticsearch']['address'],
                           port=conf['elasticsearch']['port'],
                           use_ssl=conf['elasticsearch']['ssl'],
                           http_auth=(
                               credentials['username'],
                               credentials['password']
                           ))
    else:
        logger.info('Connecting to ES instance at {0}:{1}'.format(
            conf['elasticsearch']['address'],
            conf['elasticsearch']['port']
        ))
        es = Elasticsearch(conf['elasticsearch']['address'],
                           port=conf['elasticsearch']['port'],
                           use_ssl=conf['elasticsearch']['ssl']
                           )
    # Check ES connection
    try:
        logger.info('Connected to ES instance {0}'.format(es.info()))
    except es_exceptions.AuthenticationException as ex:
        logger.warning('Unable to connect to ES instance: {0}'.format(ex))
    return es


def get_labels(data, ret_data=None):
    if ret_data is None:
        ret_data = []
    if 'aggs' in data:
        ret_data.append(list(data['aggs'].keys())[-1])
        get_labels(data['aggs'][ret_data[-1]], ret_data)
    return ret_data


def check_label(data, labels):
    """
    Check if any labels in labels in present in data
    :param data: dictionary to search in
    :param labels: list of labels to search for
    :return: label as string if found else None
    """
    try:
        return list(filter(lambda label: label in labels, data))[0]
    except IndexError:
        return None


def agg_gen(data, labels, count=0, ret_data=None):
    """
    Read aggregation search results and build a list of dictionaries based on result
    :param data: aggregation search results as dictionary
    :param labels: list of labels
    :param count: local recursion counter
    :param ret_data: hold the return data
    :return: aggregation results as list of dictionaries
    """
    if ret_data is None:
        ret_data = [{}]
    label = check_label([key for key in data], labels)
    if label:
        ret_data[-1].update({label: None})
        for bucket in data[label]['buckets']:
            ret_data[-1][label] = bucket['key']
            agg_gen(bucket, labels, count+1, ret_data)
            try:
                ret_data[-1].update({
                    key: value
                    for key, value in ret_data[-2].items()
                    if key not in ret_data[-1]
                })
            except IndexError:
                ret_data[-1].update({
                    key: value
                    for key, value in ret_data[-1].items()
                    if key not in ret_data[-1]
                })
    else:
        ret_data[-1].update({'doc_count': str(data['doc_count'])})
        ret_data.append({})
    if not count:
        del ret_data[-1]
    return ret_data


def get_from_dict(data, path_map):
    """
    Dynamically access nested dictionary values
    :param data: dictionary to check
    :param path_map: list of keys to search
    :return: found value else False
    """
    try:
        return reduce(operator.getitem, path_map, data)
    except KeyError:
        return False


def set_in_dict(data, path_map, value):
    """
    Dynamically set/update nested dictionary values
    :param data: dictionary to check
    :param path_map: list of keys to search
    :param value: value to set
    :return: None
    """
    try:
        get_from_dict(data, path_map[:-1])[path_map[-1]].update(value)
    except KeyError:
        get_from_dict(data, path_map[:-1])[path_map[-1]] = value
    except TypeError:
        get_from_dict(data, path_map[:-1])[path_map[-1]]['rowSpan'] = value(get_from_dict(data, path_map[:-1])[path_map[-1]]['rowSpan'])


def table_gen(data, labels, links):
    """
    Calculate HTML table layout based on filtered aggregation results
    :param data: list of dictionaries
    :param labels: list of labels
    :param links: list of strings
    :return: dictionary
    """
    ret_data = {}
    for t_data in data:
        for i, label in enumerate(labels):
            tmp_labels = [t_data[l] for l in labels[:i + 1]]
            if get_from_dict(ret_data, tmp_labels):
                set_in_dict(ret_data, tmp_labels, lambda x: x + 1)
            else:
                rowspan = {'rowSpan': 1}
                if not i >= len(labels):
                    if labels[i] in links:
                        tmp = links[labels[i]].format('%22' + tmp_labels[i].rstrip() + '%22')
                        rowspan.update({'link': tmp})
                set_in_dict(ret_data, tmp_labels, rowspan)
    return ret_data


def build_table(data, labels, table_name=None, ret_data=None, count=0):
    if not count:
        ret_data = [
            ['<table id="agg_table">\n'],
            [
                '<tr>\n',
                '<th colspan="{colspan}">{table_name}</th>'.format(
                    colspan=len(labels) + 1,
                    table_name=table_name
                ),
                '</tr>\n'
            ],
            ['<tr>\n'] + ['<th>{0}</th>\n'.format(label) for label in labels] + ['</tr>\n'],
            ['<tr>\n']
        ]

    for key, value in data.items():
        if 'link' in value:
            ret_data[-1].append(
                '<td rowspan={0}>\n<a href="{1}" target="_blank">{2}</a>\n</td>\n'.format(
                    value['rowSpan'],
                    value['link'],
                    key.rstrip()
                ))
            del value['link']
        else:
            ret_data[-1].append('<td rowspan={0}>{1}</td>\n'.format(
                value['rowSpan'],
                key.rstrip()
            ))
        del value['rowSpan']
        if len(value):
            build_table(data=value, labels=labels, ret_data=ret_data, count=count + 1)
        else:
            ret_data[-1].append('</tr>\n')
            ret_data.append(['<tr>\n'])
            return
    if not count:
        del ret_data[-1]
        ret_data.append(['</table>\n'])
    return ret_data


def build_mail(sender, recipient, subject, body, cc=None):
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    if cc is not None:
        msg['Cc'] = cc

    if isinstance(recipient, list):
        msg['To'] = ', '.join(recipient)
    else:
        msg['To'] = recipient

    mail_body = str(conf['mail']['body']).format(
        style=str(conf['mail']['style']),
        table=body
    )
    msg.attach(MIMEText(mail_body, 'html'))
    return msg


def main():
    for creds in conf['elasticsearch']['credentials']:
        mail_table = []
        es = es_instance(creds)

        for i, q_data in enumerate(conf['query']['queries']):
            query = q_data['body']
            labels = get_labels(query)
            try:
                logger.debug('Starting search at index {index} using query: {query}'.format(index=q_data['index'], query=query))
                search = dict(es.search(index=q_data['index'], body=query))
                logger.debug('Got search result from es instance: {result}'.format(result=str(search)))
            except es_exceptions.RequestError as ex:
                logger.warning('Unable to complete search: {0}'.format(ex))

            agg_data = agg_gen(search['aggregations'], labels)
            table_data = table_gen(agg_data, labels + ['doc_count'], q_data['links'])
            html_table = build_table(table_data, labels + ['Total events'], q_data['name'])

            mail_table.append(
                [
                    creds['subject'] + ' ({0})'.format(q_data['name']),
                    ''.join(cell for row in html_table for cell in row)
                ]
            )

        if conf['query']['mode'] == 'append':
            msg = build_mail(
                sender=creds['mail_from'],
                recipient=creds['mail_to'],
                subject=creds['subject'],
                cc='',
                body='<br><br>\n'.join([table[-1] for table in mail_table])
            )
            with smtplib.SMTP(conf['mail']['server']) as ms:
                ms.sendmail(
                    creds['mail_from'],
                    msg.as_string()
                )
        elif conf['query']['mode'] == 'separate':
            for table in mail_table:
                msg = build_mail(
                    sender=creds['mail_from'],
                    recipient=creds['mail_to'],
                    subject=table[0],
                    cc='',
                    body=table[-1]
                )
                with smtplib.SMTP(conf['mail']['server']) as ms:
                    ms.sendmail(
                        creds['mail_from'],
                        creds['mail_to'],
                        msg.as_string()
                    )
    return

if __name__ == '__main__':
    main()

