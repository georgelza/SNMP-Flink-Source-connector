# Flink Libraries

The following directories will house our Java libraries required by flink. Normally we'd include these in the Dockerfile, but in this case we place them here and then mount the directories into the containers at run time.

This makes it simpler to add/remove libraries as we simply have to restart the flink container and not rebuild it.

As the Jobmanager, Taskmanager and sql-client all use the same libraries they can all use this one set, thus also reducing their image sizes.

The various files are downloaded by executing the `getlibs.sh` file located in the `<root>/devlab/` directory.