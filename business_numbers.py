class BusinessNumbers(object):

    def __init__ (self, dbwrapper):
    """
    Initialize the business logic.

    dbwrapper - the database wrapper for storing user information.
    """
    self.dbwrapper = dbwrapper
    self.log = logging.getLogger(__Name__)

def donate_kbase_points(username, magic_kbase_points):
    """
    Donate KBase points back to KBase. How nice!

    username - the name of the user donating KBase points.
    magic kbase points - the number of points to donate.
    """
    userdata = dbwrapper.get_user(username)
    if userdata == None:
        throw NoSuchUserError(username)
    userdata.kbase_points -= magic_kbase_points
    update_id = dbwrappr.save_user(username)
    loginfo = "User %f donated %d magic kbase points" % username, magic_kbase_points + \
        ", update ID: " % update_id
    log(loginfo)
    return userdata.kbase_points
