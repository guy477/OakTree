import os
import json

class cm_environment():
    def __init__(self):
        # Specify the directory path
        self.directory = '/home/im_archaic/'

        # Get a list of all directories within the specified directory
        self.devs = [name for name in os.listdir(self.directory) if os.path.isdir(os.path.join(self.directory, name))]
        
        # STAGING IS NOT A DEVELOPER
        self.devs.remove('STAGING')

        self.cur_env = None
        self.cur_dev = None
        self.cur_dir = None
        self.paths = {}
        pass

    def get_environment(self):
        cur_dir = os.getcwd().split('\\')
        
        # Three cases: Development, STAGING, and Production.
        # Development is assumed to always take place using a network connection to a user-directory on the Task-Server's C-Drive.
        # STAGING is assumed to always take place on the Task-Servers E-Drive.
        # Production is assumed to always take place ont he Task-Servers E-Drive.
        
        print(cur_dir)
        cur_dev = 'TPRATT'
        # if cur_dir[1] == DEV; we're running stag or dev on task server; otherwise cur_dir[1] == production or the current user running the environment
        cur_env = cur_dir[1] if cur_dir[1] != 'DEV' else cur_dir[2]
        cur_db_lib = 'E:/Production/_RES/paths.json'
        if cur_env in self.devs:
            cur_dev = cur_env
            cur_env = 'Development'
            cur_db_lib = self.directory + cur_dev + '/_RES/paths.json'

        elif 'STAGING' == cur_env:
            cur_dev = 'TPRATT'
            cur_env = 'STAGING'
            cur_db_lib = self.directory + cur_env + '/_RES/paths.json'

        # else:
        #   in production; already defined above.

        self.paths = json.load(open(cur_db_lib))


        # else:
        #   the environment will automatically resolve based on the \\task\e$ directory.

        # translate the current working directory - so we can reference global definitions relative to the current environment..
        # Something about this feels messy..
        for i in range(len(cur_dir)):
            if  cur_dir[i] == 'Production' or cur_dir[i] == 'STAGING' or cur_dir[i] in self.devs:

                cur_dir = cur_dir[:i + 1]
                cur_dir = "\\".join(cur_dir)
                break
        
        # Set global variables:

        self.cur_env = cur_env
        self.cur_dev = cur_dev
        self.cur_dir = cur_dir

        return (cur_env, (cur_dev, self.paths), cur_dir)
            

# NOTE: Kinda just leave this uncommented b/c it's nice to see what enviornment we're at the start of a program's run
cm = cm_environment().get_environment()
print(cm)