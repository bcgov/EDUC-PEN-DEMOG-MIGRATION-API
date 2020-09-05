envValue=$1
APP_NAME=$2
OPENSHIFT_NAMESPACE=$3
APP_NAME_UPPER=${APP_NAME^^}
SOAM_KC_REALM_ID="master"
TZVALUE="America/Vancouver"

oc project "$OPENSHIFT_NAMESPACE"-"$envValue"

DB_JDBC_CONNECT_STRING=$(oc -o json get configmaps "${APP_NAME}"-"${envValue}"-setup-config | sed -n 's/.*"DB_JDBC_CONNECT_STRING": "\(.*\)"/\1/p')
DB_PWD=$(oc -o json get configmaps "${APP_NAME}"-"${envValue}"-setup-config | sed -n "s/.*\"DB_PWD_${APP_NAME_UPPER}\": \"\(.*\)\"/\1/p")
DB_USER=$(oc -o json get configmaps "${APP_NAME}"-"${envValue}"-setup-config | sed -n "s/.*\"DB_USER_${APP_NAME_UPPER}\": \"\(.*\)\"/\1/p")



###########################################################
#Setup for config-map
###########################################################

echo
echo Creating config map "$APP_NAME"-config-map
oc create -n "$OPENSHIFT_NAMESPACE"-"$envValue" configmap "$APP_NAME"-config-map --from-literal=TZ=$TZVALUE --from-literal=JDBC_URL="$DB_JDBC_CONNECT_STRING" --from-literal=ORACLE_USERNAME="$DB_USER" --from-literal=ORACLE_PASSWORD="$DB_PWD"  --from-literal=SPRING_WEB_LOG_LEVEL=INFO --from-literal=APP_LOG_LEVEL=TRACE --from-literal=SPRING_BOOT_AUTOCONFIG_LOG_LEVEL=INFO --from-literal=SPRING_SHOW_REQUEST_DETAILS=false --from-literal=HIBERNATE_STATISTICS=false --dry-run -o yaml | oc apply -f -
echo
echo Setting environment variables for "$APP_NAME"-$SOAM_KC_REALM_ID application
oc project "$OPENSHIFT_NAMESPACE"-"$envValue"
oc set env --from=configmap/"$APP_NAME"-config-map dc/"$APP_NAME"-$SOAM_KC_REALM_ID
