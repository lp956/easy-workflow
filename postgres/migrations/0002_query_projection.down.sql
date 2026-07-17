-- Removes only query projection artifacts; command-side aggregate tables and facts remain untouched.
DROP TABLE IF EXISTS easy_workflow_participation_projection;
DROP TABLE IF EXISTS easy_workflow_instance_projection;
