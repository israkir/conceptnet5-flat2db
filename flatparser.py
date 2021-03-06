import os
import sys
import json
import codecs 
import ast
import psycopg2
import psycopg2.extras

from settings import DB_HOST, DB_NAME, DB_USER, DB_PASSWD


DEBUG = False


def get_conn():
    try:    
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWD)
        return conn
    except:
        print '[error] Unable to connect database.'


def traverse_dir(path):
    for path, dirs, files in os.walk(path):
        for filename in files:
            fullpath = os.path.join(path, filename)
            fid = insert_source_file(filename)
            f = codecs.open(fullpath, 'r', encoding='utf-8')
            
            # quick-and-dirty hack for not repeating insertion of a relation
            # because all relations are inserted in english only
            insert_language('en')
            
            print 'parsing file contents'
            counter = 1
            for json_line in f:
                print '%s : json_line: %s' % (counter, json_line)
                counter += 1
                insert_all_attributes(json_line, fid)
                print '\n--------------------\n'


def insert_source_file(filename):
    print 'inserting source file: %s' % filename
    
    conn = get_conn()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    cmd = cursor.mogrify(
        '''
        SELECT id from source_file
        WHERE filename = %s
        '''
    )

    cursor.execute(cmd, [filename])
    row = cursor.fetchone()

    if not row:
        if not DEBUG:
            cmd = cursor.mogrify(
                '''
                INSERT INTO source_file (filename)
                VALUES (%s)
                '''
            )

            cursor.execute(cmd, [filename])
            cursor.execute('''SELECT currval('source_file_id_seq');''')
            fid = cursor.fetchone()[0]
    else:
        fid = row[0]
        print 'source file already exists: %s | id: %s' % (filename, fid)

    
    cursor.close()
    conn.commit()

    return fid
    

def insert_all_attributes(json_str, fid):
    print 'parsing all attributes...'
    data = json.loads(json_str)

    if '/' in data['start']:
        start_lang = data['start'].split('/')[2]
    else:
        start_lang = 'en'
        
    insert_language(start_lang)
    
    if '/' in data['end']:
        end_lang = data['end'].split('/')[2]
    else:
        end_lang = 'en'
    
    insert_language(end_lang)
   
    dataset = data['dataset']
    insert_dataset(dataset)

    license = data['license']
    insert_license(data['license'])

    source_list = ast.literal_eval(str(data['sources']))
    sid_list = []
    for source in source_list:
        sid = insert_source(source)
        sid_list.append(sid)

    # verbosity.json specific formatting
    # attributes for these don't have namespace
    # and multi-word concepts don't have underscore
    if '/' in data['start']:
        start = data['start'].encode('utf-8')
    else:
        start = ('/c/en/' + data['start'].replace(' ', '_')).encode('utf-8')
    
    start_id = insert_concept(start, start_lang)

    rel = data['rel'].encode('utf-8')
    rel_id = insert_concept(rel, 'en')

    # verbosity.json specific formatting
    # attributes for these don't have namespace
    # and multi-word concepts don't have underscore
    if '/' in data['end']:
        end = data['end'].encode('utf-8') 
    else:
        end = ('/c/en/' + data['end'].replace(' ', '_')).encode('utf-8') 
    
    end_id = insert_concept(end, end_lang)

    insert_assertion(start_id, rel_id, end_id)

    weight = data['weight']
    if 'score' in data:
        score = data['score']
    else:
        score = None
    surface_text = data['surfaceText'].encode('utf-8')
    uri = data['uri'].encode('utf-8')
    raw_assertion_id, assertion_exist = insert_raw_assertion(start_id, rel_id, end_id, 
        license, dataset,surface_text, weight, score, uri, fid, sid_list) 

    if not assertion_exist:
        insert_assertion_source(raw_assertion_id, sid_list)


def insert_language(lang):
    print 'inserting language: %s' % lang
    
    if not DEBUG:
        conn = get_conn()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cmd = cursor.mogrify(
            '''
            SELECT id FROM language
            WHERE name = %s
            '''
        )
        
        cursor.execute(cmd, [lang])
        row = cursor.fetchone()

        if not row:
            cmd = cursor.mogrify(
                '''
                INSERT INTO language (name)
                VALUES (%s)
                '''
            )
            cursor.execute(cmd, [lang])
        else:
            print 'language already exists: %s' % (lang)

        cursor.close()
        conn.commit()
    

def insert_dataset(dataset):
    print 'inserting dataset: %s' % dataset
    
    if not DEBUG:
        conn = get_conn()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cmd = cursor.mogrify(
            '''
            INSERT INTO dataset (name)
            SELECT name FROM dataset
            UNION
            VALUES (%s)
            EXCEPT
            SELECT name FROM dataset
            '''
        )

        cursor.execute(cmd, [dataset])
        cursor.close()
        conn.commit()


def insert_license(license):
    print 'inserting license: %s' % license
    
    if not DEBUG:
        conn = get_conn()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cmd = cursor.mogrify(
            '''
            INSERT INTO license (name)
            SELECT name FROM license
            UNION
            VALUES (%s)
            EXCEPT
            SELECT name FROM license
            '''
        )

        cursor.execute(cmd, [license])
        cursor.close()
        conn.commit()


def insert_source(source):
    print 'inserting source'
    # print 'inserting source: %s' % source
    
    conn = get_conn()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cmd = cursor.mogrify(
        '''
        SELECT id FROM source
        WHERE name = %s
        '''
    )

    cursor.execute(cmd, [source])
    row = cursor.fetchone()

    if not row:
        if not DEBUG:
            cmd = cursor.mogrify(
                '''
                INSERT INTO source (name)
                VALUES (%s)
                '''
            )
            cursor.execute(cmd, [source])
            cursor.execute('''SELECT currval('source_id_seq');''')
            sid = cursor.fetchone()[0]
    else:
        sid = row[0]
        print 'source already exists - id: %s' % (sid)

    cursor.close()
    conn.commit()

    return sid


def insert_concept(concept, lang):
    print 'inserting concept'
    # print 'inserting concept: %s | in lang: %s' % (concept.decode('utf-8'), lang.decode('utf-8'))
    
    conn = get_conn()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cmd = cursor.mogrify(
        '''
        SELECT id FROM concept
        WHERE uri = %s AND language_id = (
            SELECT id FROM language 
            WHERE name = %s
        )
        '''
    )

    cursor.execute(cmd, [concept, lang])
    row = cursor.fetchone()
    
    if not row:
        if not DEBUG:
            cmd = cursor.mogrify(
                '''
                INSERT INTO concept (uri, language_id)
                    VALUES (%s, (SELECT id FROM language WHERE name = %s))
                '''
            )
            cursor.execute(cmd, [concept, lang])
            cursor.execute('''SELECT currval('concept_id_seq');''')
            row_id = cursor.fetchone()[0]
    else:
        row_id = row[0]
        print 'concept already exists - id: %s' % (row_id)
    
    cursor.close()
    conn.commit()

    return row_id


def insert_assertion(start_id, rel_id, end_id):
    print 'inserting assertion: %s %s %s' % (start_id, rel_id, end_id)
    
    if not DEBUG:
        conn = get_conn()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        cmd = cursor.mogrify(
            '''
            SELECT id FROM assertion
            WHERE start_id = %s AND relation_id = %s AND end_id = %s
            '''
        )

        cursor.execute(cmd, [start_id, rel_id, end_id])
        rows = cursor.fetchall()
        
        if not rows:
            cmd = cursor.mogrify(
                '''
                INSERT INTO assertion (start_id, relation_id, end_id)
                VALUES (%s, %s, %s)
                '''
            )
            cursor.execute(cmd, [start_id, rel_id, end_id])
        else:
            print 'assertion already exists: %s %s %s' % (start_id, rel_id, end_id)

        cursor.close()
        conn.commit()


def insert_raw_assertion(start_id, rel_id, end_id, license, dataset, surface_text, weight, score, uri, fid, sid_list):
    print 'inserting raw assertion'
    #print 'inserting raw assertion: %s %s %s | license: %s | dataset: %s | surface_text: %s | uri: %s | fid: %s' % (
    #    start_id, rel_id, end_id, license, dataset, surface_text, uri,fid)

    conn = get_conn()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
   
    # check raw assertion does exist with first parameters
    cmd = cursor.mogrify(
        '''
        SELECT id FROM raw_assertion
        WHERE assertion_id = (SELECT id FROM assertion WHERE start_id = %s AND relation_id = %s AND end_id = %s)
            AND license_id = (SELECT id FROM license WHERE name = %s)
            AND dataset_id = (SELECT id FROM dataset WHERE name = %s)
            AND surface_text = %s
            AND weight = %s
            AND score IS NOT DISTINCT FROM %s
            AND uri = %s
            AND source_file_id = %s
        '''
    )
   
    cursor.execute(cmd, [start_id, rel_id, end_id, license, dataset, surface_text, weight, score, uri, fid])
    old_raw_assertion_ids = cursor.fetchall()

    # if there are some raw assertions for this surface text, check whether it is from the same resource
    if old_raw_assertion_ids:
        print 'raw assertion does exist. checking whether it is from the same source...'
        for old_raw_assertion_id in old_raw_assertion_ids:
            cmd = cursor.mogrify(
                '''
                SELECT * FROM assertion_source
                WHERE raw_assertion_id = %s
                '''
            )
            cursor.execute(cmd, [old_raw_assertion_id[0]])
            assertion_sources = cursor.fetchall()

            if assertion_sources:
                counter = 0
                for assertion_source in assertion_sources:
                    cmd = cursor.mogrify(
                        '''
                        SELECT id FROM source
                        WHERE id = %s
                        '''
                    )
                    cursor.execute(cmd, [assertion_source[2]])
                    source_id = cursor.fetchone()[0]
                    if source_id in sid_list:
                        counter += 1 

                if len(assertion_sources) == counter:
                    print 'raw assertion identically exist!'
                    assertion_exist = True
                    return old_raw_assertion_id[0], assertion_exist

                if len(assertion_sources) != counter:
                    if not DEBUG:
                        cmd = cursor.mogrify(
                            '''
                            INSERT INTO raw_assertion (assertion_id, license_id, dataset_id, surface_text, weight, score, uri, source_file_id)
                            VALUES (
                                (SELECT id FROM assertion WHERE start_id = %s AND relation_id = %s AND end_id = %s), 
                                (SELECT id FROM license WHERE name = %s), 
                                (SELECT id FROM dataset WHERE name = %s),
                                %s, %s, %s, %s, %s
                            )
                            '''
                        )

                        cursor.execute(cmd, [start_id, rel_id, end_id, license, dataset, surface_text, weight, score, uri, fid])
                    
                        cursor.execute('''SELECT currval('raw_assertion_id_seq');''')
                        raw_assertion_id = cursor.fetchone()[0]
                    
                        cursor.close()
                        conn.commit()
                    
                        assertion_exist = False
                        return raw_assertion_id, assertion_exist

    # if there is no raw assertion for this surface text, add a new raw assertion
    else:
        if not DEBUG:
            cmd = cursor.mogrify(
                '''
                INSERT INTO raw_assertion (assertion_id, license_id, dataset_id, surface_text, weight, score, uri, source_file_id)
                VALUES (
                    (SELECT id FROM assertion WHERE start_id = %s AND relation_id = %s AND end_id = %s), 
                    (SELECT id FROM license WHERE name = %s), 
                    (SELECT id FROM dataset WHERE name = %s),
                    %s, %s, %s, %s, %s
                )
                '''
            )

            cursor.execute(cmd, [start_id, rel_id, end_id, license, dataset, surface_text, weight, score, uri, fid])
        
            cursor.execute('''SELECT currval('raw_assertion_id_seq');''')
            raw_assertion_id = cursor.fetchone()[0]
        
            cursor.close()
            conn.commit()
        
            assertion_exist = False
            return raw_assertion_id, assertion_exist
    
    return surface_text


def insert_assertion_source(raw_assertion_id, sid_list):
    if not DEBUG:
        conn = get_conn()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        for sid in sid_list:
            cmd = cursor.mogrify(
                '''
                INSERT INTO assertion_source (raw_assertion_id, source_id)
                VALUES (%s, %s)
                '''
            )
            cursor.execute(cmd, [raw_assertion_id, sid])
            
        cursor.close()
        conn.commit()


def main():
    if len(sys.argv) < 2:
        print 'Usage: $ python %s [flat-files-folder-path]\nExiting...' % sys.argv[0]
        sys.exit()
    traverse_dir(sys.argv[1])
    

if __name__ == '__main__':
    main()
