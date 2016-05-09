from __future__ import absolute_import

from mw.xml_dump import Iterator
from mw.xml_dump import functions
from mw.lib import persistence
import csv
import re
from IPy import IP
from collections import OrderedDict
import argparse
from datetime import datetime

def main():

    logfile = "botlike/log_botlike_2015_09_05.txt"
    ap = argparse.ArgumentParser()
    ap.add_argument('filename', help='provide the wikipedia xml bz2 dump filename')
    args = ap.parse_args()
    with open(logfile, "a") as fout:
        fout.write("Starting on {0} at {1}.\n".format(args.filename, str(datetime.now())))
    # Initialize:
    page_ids_file = "resources/talk_page_ids_counties.csv"
    outputpagescsv = "botlike/spawiki_botlike_pages_pwr.csv"
    outputuserscsv = "botlike/spawiki_botlike_editors_geography.csv"
    outputpwrcsv = "botlike/spawiki_botlike_currentpage_editors.csv"
    expected_header = ['LOCAL_ID','TITLE','TALK_ID','TALK_TITLE','GEOMETRY','county_fips',
            'TOTAL_EDITS','BOT_EDITS','UNVERIFIED_BOT_EDITS','KNOWN_SCRIPT_EDITS','ANONYMOUS_EDITS','AWB_EDITS','MINOR_EDITS','WPCLEANER_EDITS',
            'TOTAL_TOKENS','BOT_TOKENS','UNVERIFIED_BOT_TOKENS','KNOWN_SCRIPT_TOKENS','ANONYMOUS_TOKENS','AWB_TOKENS']
    with open(page_ids_file, "r") as fin:
        csvreader = csv.reader(fin)
        assert next(csvreader) == expected_header[0:6], "check header for {0}".format(page_ids_file)
        page_id_dict = {}
        found_pages_dict = {}
        users_page_edits_dict = {}
        pages_pwr_dict = {}
        for line in csvreader:
            local_id = int(line[0])
            initial_vals = {'local_id':line[0], 'title':line[1], 'talk_id':line[2], 'geometry':line[4], 'county_fips':line[5]}
            page_id_dict[local_id] = initial_vals

    # All the work:

    parse_dump(args.filename, page_id_dict, found_pages_dict, users_page_edits_dict, pages_pwr_dict, logfile)

    # Output:
    for i in range(0, len(expected_header)):
        expected_header[i] = expected_header[i].lower()
    try:
        with open(outputpagescsv, "a", encoding='utf-8', errors='backslashreplace') as fout:
            csvwriter = csv.DictWriter(fout, expected_header)
            csvwriter.writeheader()
            csvwriter.writerows(OrderedDict(sorted(found_pages_dict.items(), key=lambda t: t[0])).values())  # sort rows by page_id
    except Exception as e:
        print(e)
        print("Failed to output page edit counts")
        with open(logfile, 'a', encoding='utf-8', errors='backslashreplace') as fout:
            fout.write(str(e) + "\n")
            fout.write("Failed to output page edit counts.\n")
    with open(outputuserscsv, "a", encoding='utf-8', errors='backslashreplace') as fout:
        csvwriter = csv.writer(fout)
        csvwriter.writerow(['user_text', 'page_ids'])
        for user in users_page_edits_dict:
            try:
                csvwriter.writerow([user, str(users_page_edits_dict[user])])
            except Exception as e:
                print(e)
                try:
                    print("Failed to output user edit counts for {0}".format(user))
                    with open(logfile, "a", encoding='utf-8', errors='backslashreplace') as fout:
                        fout.write(str(e) + "\n")
                        fout.write("Failed to output user edit counts for {0}.\n".format(user))
                except:
                    print("Failed to output user edit counts for a user.")
                    with open(logfile, 'a', encoding='utf-8', errors='backslashreplace') as fout:
                        fout.write(str(e) + "\n")
                        fout.write("Failed to output user edit counts for a user.\n")
    with open(outputpwrcsv, "a", encoding='utf-8', errors='backslashreplace') as fout:
        csvwriter = csv.writer(fout)
        csvwriter.writerow(['page_id', 'user_edit_counts'])
        for page_id in pages_pwr_dict:
            try:
                csvwriter.writerow([page_id, str(pages_pwr_dict[page_id])])
            except Exception as e:
                print(e)
                print("Failed to output editors for current version of pages for page {0}".format(page_id))
                with open(logfile, "a", encoding='utf-8', errors='backslashreplace') as fout:
                    fout.write(str(e) + "\n")
                    fout.write("Failed to output editors for current version of pages for page {0}.\n".format(page_id))

def parse_dump(dump_filename, wanted_page_ids, found_pages_dict, users_page_edits_dict, pages_pwr_dict, logfile):
    '''
    Parse the given dump, processing assessments for the given
    talk page IDs.

    @param dump_filename: path to the dump file to process
    @type dump_filename: str

    @param wanted_page_ids: dictionary where keys are talk page IDs,
                            values don't matter, we're only using the
                            dict for fast lookups
    @type wanted_page_ids: dict
    '''

    # Construct dump file iterator
    dump = Iterator.from_file(functions.open_file(dump_filename))

    bots_file = 'resources/wikipedia_bots_full.txt'
    bots = {}
    try:
        with open(bots_file, 'r') as fin:
            csvreader = csv.reader(fin)
            for line in csvreader:
                bots[line[0].lower()] = True
    except:
        print("Invalid bots file - only text matching with 'bot' will be used")
        with open(logfile, "a") as fout:
            fout.write("Invalid bots file - only text regex with 'bot' followed by whitespace will be used.\n")

    scripts = ['commonsdelinker', 'conversion script']

    count = 0
    # Iterate through pages
    for page in dump:
        # skip if not a page we want to process
        if not page.id in wanted_page_ids:
            continue
        try:
            with open(logfile, "a", encoding='utf-8', errors='backslashreplace') as fout:
                fout.write(str(datetime.now()) + ": " + page.title + "\n")
            print(page.title)
        except:
            with open(logfile, 'a') as fout:
                fout.write(str(datetime.now()) + ": next spatial article.\n")
            print("next spatial article.")

        state = persistence.State()

        count += 1
        counts_dict = {'total_edits':0, 'bot_edits':0, 'unverified_bot_edits':0, 'known_script_edits':0,
                       'anonymous_edits':0, 'awb_edits':0, 'minor_edits':0, 'wpcleaner_edits':0}

        # Iterate through a page's revisions
        for revision in page:
            # skip if there's no content
            if not revision.text:
                continue

            if revision.comment and 'awb' in revision.comment.lower():
                pwr = state.process(revision.text, revision='awb')
            else:
                pwr = state.process(revision.text, revision=revision.contributor.user_text)

            counts_dict['total_edits'] += 1
            try:
                if revision.contributor.user_text:
                    process_rev(revision, counts_dict, bots, scripts, users_page_edits_dict, page.id)
            except:
                try:
                    print("Error in revision.contributor.user_text {0} for page {1}".format(revision.contributor.user_text, page.title))
                    with open(logfile, "a") as fout:
                        fout.write("Error in revision.contributor.user_text {0} for page {1}\n".format(revision.contributor.user_text, page.title))
                except:
                    print("Error in a revision.contributor.user_text for a page.")
                    with open(logfile, "a") as fout:
                        fout.write("Error in a revision.contributor.user_text for a page.")

        found_pages_dict[page.id] = wanted_page_ids[page.id]
        found_pages_dict[page.id].update(counts_dict)

        current_state = {'total_tokens':0, 'bot_tokens':0, 'unverified_bot_tokens':0, 'known_script_tokens':0,
                         'anonymous_tokens':0, 'awb_tokens':0}

        for tk in pwr[0]:  # loop through tokens in current state of the page
            current_state['total_tokens'] += 1
            try:
                if tk.revisions[0]:
                    process_current_page(tk.revisions[0].lower(), current_state, bots, scripts, pages_pwr_dict, page.id)
            except:
                try:
                    print("Error in processing token {0} for page {1}".format(tk.text, page.id))
                    with open(logfile, "a", encoding='utf-8', errors='backslashreplace') as fout:
                        fout.write("Error in processing token {0} for page {1}.\n".format(tk.text, str(page.id)))
                except:
                    print("Error in processing a token for page {0}".format(page.id))
                    with open(logfile, "a") as fout:
                        fout.write("Error in processing a token for page {0}.\n".format(page.id))
        found_pages_dict[page.id].update(current_state)


    # ok, done
    return

def process_rev(revision, counts_dict, bots, scripts, update_dict, page_id):
    user_text = revision.contributor.user_text.lower()
    if revision.minor:
        counts_dict['minor_edits'] += 1
    if user_text in bots:
        counts_dict['bot_edits'] += 1
    elif re.search('bot(\s|$|_)', user_text, re.I):
        counts_dict['unverified_bot_edits'] += 1
    elif user_text in scripts:
        counts_dict['known_script_edits'] += 1
    else:
        try:
            IP(user_text)
            counts_dict['anonymous_edits'] += 1
        except:
            if user_text in update_dict:
                if page_id in update_dict[user_text]:
                    update_dict[user_text][page_id] += 1
                else:
                    update_dict[user_text][page_id] = 1
            else:
                update_dict[user_text] = {page_id:1}
            if revision.comment and 'awb' in revision.comment.lower():
                counts_dict['awb_edits'] += 1
            elif revision.comment and 'wpcleaner' in revision.comment.lower():
                counts_dict['wpcleaner_edits'] += 1


def process_current_page(user_text, counts_dict, bots, scripts, update_dict, page_id):
    if user_text == 'awb':
        counts_dict['awb_tokens'] += 1
    elif user_text in bots:
        counts_dict['bot_tokens'] += 1
    elif re.search('bot(\s|$|_)', user_text, re.I):
        counts_dict['unverified_bot_tokens'] += 1
    elif user_text in scripts:
        counts_dict['known_script_tokens'] += 1
    else:
        try:
            IP(user_text)
            counts_dict['anonymous_tokens'] += 1
        except:
            if page_id in update_dict:
                if user_text in update_dict[page_id]:
                    update_dict[page_id][user_text] += 1
                else:
                    update_dict[page_id][user_text] = 1
            else:
                update_dict[page_id] = {user_text:1}



if __name__ == "__main__":
    main()