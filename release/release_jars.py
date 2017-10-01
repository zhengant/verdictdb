import git
import re
import os
from subprocess import call
from update_build_number import current_version

script_dir = os.path.dirname(os.path.realpath(__file__))
verdict_doc_dir = os.path.join(script_dir, "../../verdict-doc")
verdict_site_dir = os.path.join(script_dir, "../../verdict-site")
sourceforge_scp_base_url = "frs.sourceforge.net:/home/frs/project/verdict"
sourceforge_download_base_url = "https://sourceforge.net/projects/verdict/files"
push_to_git = False

def get_version_string(j_version):
    return "%s.%s.%s" % (j_version['major'], j_version['minor'], j_version['build'])

def get_cli_zip_filename(j_version):
    return 'verdict-cli-%s.zip' % get_version_string(j_version)

def remove_cli_zip(j_version):
    print 'removes cli zip file.'
    call(['rm', get_cli_zip_filename(j_version)])

def zip_command_line_interface(j_version):
    call(['zip', '-r', get_cli_zip_filename(j_version), 'README.md', 'LICENSE', 'bin/veeline',
          'jars/verdict-shell-%s.jar' % get_version_string(j_version),
          'jars/verdict-jdbc-%s.jar' % get_version_string(j_version),
          'conf/log4j.properties'])

def update_verdict_site(j_version):
    """
    The download page in the verdict documentation should include
    1. the correct link to the repository
    2. the correct compiled jar files.
    """
    print 'updates verdict site.'
    g = git.cmd.Git(verdict_site_dir)
    g.pull()
    verdict_site_conf_file = os.path.join(verdict_site_dir, '_config.yml')
    sf_url = 'https://sourceforge.net/projects/verdict/files/%d.%d' % (j_version['major'], j_version['minor'])
    updated_lines = []
    for l in open(verdict_site_conf_file):
        u = l;
        if re.match("url:.*", l):
            u = 'url: "http://verdictdb.org/"\n'
        if re.match("version:.*", l):
            u = 'version: %s\n' % get_version_string(j_version)
        if re.match("verdict_core_jar_name:.*", l):
            u = 'verdict_core_jar_name: verdict-spark-lib-%s.jar\n' % get_version_string(j_version)
        if re.match("verdict_core_jar_url:.*", l):
            u = 'verdict_core_jar_url: %s/verdict-spark-lib-%s.jar/download\n' % (sf_url, get_version_string(j_version))
        if re.match("verdict_jdbc_jar_name:.*", l):
            u = 'verdict_jdbc_jar_name: verdict-jdbc-%s.jar\n' % get_version_string(j_version)
        if re.match("verdict_jdbc_jar_url:.*", l):
            u = 'verdict_jdbc_jar_url: %s/verdict-jdbc-%s.jar/download\n' % (sf_url, get_version_string(j_version))
        if re.match("verdict_command_line_zip_name:.*", l):
            u = 'verdict_command_line_zip_name: verdict-cli-%s.zip\n' % get_version_string(j_version)
        if re.match("verdict_command_line_zip_url:.*", l):
            u = 'verdict_command_line_zip_url: %s/verdict-cli-%s.zip/download\n' % (sf_url, get_version_string(j_version))
        if re.match("verdict_veeline_jar_name:.*", l):
            u = 'verdict_veeline_jar_name: verdict-shell-%s.jar\n' % get_version_string(j_version)
        updated_lines.append(u)
    with open(verdict_site_conf_file, 'w') as fout:
        fout.write("".join(updated_lines))
        fout.write("\n")
    try:
        g.execute(['git', 'commit', '-am', 'version updated to %s' % get_version_string(j_version)])
    except git.exc.GitCommandError:
        pass
    if push_to_git:
        g.push()

def update_verdict_doc(j_version):
    """
    The download page in the verdict documentation should include
    1. the correct link to the repository
    2. the correct compiled jar files.
    """
    print 'updates verdict documentation.'
    g = git.cmd.Git(verdict_doc_dir)
    g.pull()
    verdict_doc_conf_file = os.path.join(verdict_doc_dir, 'conf.py')
    version_str = '%s.%s.%s' % (j_version['major'], j_version['minor'], j_version['build'])
    lines = [l for l in open(verdict_doc_conf_file)]
    updated_lines = []
    for l in lines:
        result1 = re.match("version = .*", l)
        result2 = re.match("release = .*", l)
        if result1 is None and result2 is None:
            updated_lines.append(l)
        elif result1:
            updated_lines.append("version = u'%s'\n" % (version_str))
        elif result2:
            updated_lines.append("release = u'%s'\n" % (version_str))
    with open(verdict_doc_conf_file, 'w') as conf_file_out:
        conf_file_out.write("".join(updated_lines))
    try:
        g.execute(['git', 'commit', '-am', 'version updated to %s' % version_str])
    except git.exc.GitCommandError:
        pass
    if push_to_git:
        g.push()

def get_path_to_files_to_upload(j_version):
    paths = []
    jars_dir = os.path.join(script_dir, '../jars')
    get_version_string(j_version)
    paths.append(os.path.join(jars_dir, 
        'verdict-spark-lib-%s.jar' % get_version_string(j_version)))
    paths.append(os.path.join(jars_dir, 
        'verdict-jdbc-%s.jar' % get_version_string(j_version)))
    paths.append(get_cli_zip_filename(j_version))
    return paths

def create_sourceforge_dir_if_not_exists(j_version):
    print 'creates a version-specific folder if not exists.'
    v = "%d.%d" % (j_version['major'], j_version['minor'])
    mkdir_str = "mkdir -p /home/frs/project/verdict/%s" % v
    call(['ssh', "yongjoop,verdict@shell.sourceforge.net", 'create'])
    call(['ssh', "yongjoop,verdict@shell.sourceforge.net", mkdir_str])

def upload_file_to_sourceforge(path, j_version):
    """
    Upload
    1. core jar
    2. jdbc jar
    3. command-line interface zip
    """
    major = j_version['major']
    minor = j_version['minor']
    major_minor = "%d.%d" % (major, minor)
    target = 'yongjoop@%s/%s/' % (sourceforge_scp_base_url, major_minor)
    print 'uploads %s to the %s.' % (path, target)
    call(['scp', path, target])

if __name__ == "__main__":
    j_version = current_version()
    zip_command_line_interface(j_version)
    create_sourceforge_dir_if_not_exists(j_version)
    file_paths = get_path_to_files_to_upload(j_version)
    for p in file_paths:
        upload_file_to_sourceforge(p, j_version)
    update_verdict_doc(j_version)
    update_verdict_site(j_version)
    remove_cli_zip(j_version);