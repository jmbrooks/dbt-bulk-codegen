#!/bin/bash

while getopts d:o:p flag
do
    case "${flag}" in
        d) directory=${OPTARG};;
        o) output_directory=${OPTARG};;
        p) preview=${OPTARG};;
    esac
done

model_yaml_codegen() {
    codegen_command="dbt run-operation generate_model_yaml --args '{"\""model_name"\"": "\""$1"\""}'"
    echo "${codegen_command}"
    eval "$codegen_command"
}

models=()
files=$(find ${directory} -type f -name '*.sql' -print0 | sort -z | xargs -0 echo)
echo "Fetching all model files in directory: ${directory}..."

file_number=1
output_file=${output_directory:-schema.yml}
echo "Attempting to write dbt model YAML codegen output to file ${output_file}..."

for file in {$files}
do
    echo "Fetching model and its associated YAML for file: ${file}"
    file_prefix=${file%.*}
    model=${file_prefix##*/}
    models=( "${models[@]}" ${model_name} )

    model_yaml_output="$(model_yaml_codegen $model)"
    # echo "${model_yaml_output}"

    if [ $file_number -eq 1 ]
    then
        echo -e "version: 2\n" >> $output_file
        echo -e "models:" >> $output_file
    else
        echo "" >> $output_file
    fi

    echo "${model_yaml_output}" | sed -n '/models:/,$p' | awk 'NR > 1' >> $output_file
    file_number=$((file_number + 1))
done

echo "${models[@]}"
echo "Done."
