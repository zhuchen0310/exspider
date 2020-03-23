compose_file='docker-compose.yml'

if [ $# -eq 2 ];then 

    compose_file=$2
fi

case $1 in
    "run")
        docker-compose -f ${compose_file} up;;

    "restart")
        docker-compose -f ${compose_file} up --no-recreate;;

    "rm")
        docker-compose -f ${compose_file} rm;;

    "drun")
        docker-compose -f ${compose_file} up -d;;
    "logs")
        docker-compose -f ${compose_file} logs;;

    *)
        echo -e 'usage ./docker.sh {run|restart|rm|logs|drun} {compose_file}';;
esac

