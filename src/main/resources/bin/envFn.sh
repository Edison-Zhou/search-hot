#!/bin/bash

load_args()
{
    for x in $*
    do
        if [[ $x == --*=*  ]]
        then
            x=`echo $x|tr -d '\n'|tr -d '\r'|tr -d '-'`
            arr=(${x/=/ })
            propName=${arr[0]}
            propValue=${arr[1]}
            propName=${propName//./_}
            propName=${propName//-/_}
            eval "$propName=$propValue"
            echo arg: $propName=$propValue
        fi
    done
}

#load properties to bash env
#params: $1 inFile
load_properties()
{
    for line in `cat $1|grep -v ^[#]`
    do

            if [[ $line == *=*  ]]
            then
                line=`echo $line|tr -d ' '|tr -d '\n'|tr -d '\r'`
                arr=(${line/=/ })
                propName=${arr[0]}
                propValue=${arr[1]}
                propName=${propName//./_}
                propName=${propName//-/_}
                eval "$propName=$propValue"
                echo propline: $propName=$propValue
            fi
    done
}

#convert properties
#params: $1 inFile, $2 outFile
resolve_properties()
{
    > $2
    for line in `cat $1|grep -v ^[#]`
    do
	    if [[ $line == *=*  ]]
            then
                line=`echo $line|tr -d ' '|tr -d '\n'|tr -d '\r'`
		        arr=(${line/=/ })
		        eval echo "${arr[0]}=${arr[1]}" >> $2
            fi
    done
}

get_fullpath()
{

    #鍒ゆ柇鏄惁鏈夊弬鏁�,姣斿褰撳墠璺緞涓�/home/user1/workspace   鍙傛暟涓� ./../dir/hello.c
    if [ -z $1 ]
    then
        return 1
    fi
    relative_path=$1

    #鍙栧墠闈竴閮ㄥ垎鐩綍,姣斿  ./../../ ,  ../../ 绛�, 鍦ㄨ繖閲岃皟鐢╟d鍛戒护鏉ヨ幏鍙栬繖閮ㄥ垎璺緞鐨勭粷瀵硅矾寰�,鍥犱负鎸夎繖鏍峰啓鐨�,鍦ㄥ綋鍓嶈矾寰勭殑涓婄骇鐩綍鑲畾鏄瓨鍦ㄧ殑.
    #tmp_path1涓� ./..
    #tmp_fullpath1 /home/user1

    tmp_path1=$(echo $relative_path | sed -e "s=/[^\.]*$==")
    tmp_fullpath1=$(cd $tmp_path1 ;  pwd)

    #鑾峰彇鍚庨潰涓�閮ㄥ垎璺緞
    #tmp_path2涓篸ir/hello.c
    tmp_path2=$(echo $relative_path | sed -e "s=\.[\./]*[/|$]==")

    #鎷煎噾璺緞杩斿洖
    echo ${tmp_fullpath1}/${tmp_path2}
    return 0
}

