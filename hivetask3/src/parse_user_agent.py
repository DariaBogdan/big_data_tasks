#!/usr/bin/env python3
import sys
from ua_parser import user_agent_parser

def parse_ua(line):
    """ Extracts from user agent info about divice, os, browser.

    :param line: city_id and user_agent, separated by \t
    :return: city_id, device, os, browser separated by \t
    """
    city_id, ua = line.split('\t')
    parsed = user_agent_parser.Parse(ua)
    ua_os = parsed['os']['family']
    ua_browser = parsed['user_agent']['family']
    ua_device = parsed['device']['family']
    print('{}\t{}\t{}\t{}'.format(city_id, ua_device, ua_os, ua_browser))


if __name__ == 'main':
    for line in sys.stdin:
        parse_ua(line)