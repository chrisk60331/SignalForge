#!/bin/bash
# ASCII Fire Animation

COLS=$(tput cols)
ROWS=$(tput lines)
declare -a grid
declare -a chars=(" " "." ":" "*" "s" "S" "#" "$" "@")
declare -a colors=("\e[30m" "\e[31m" "\e[33m" "\e[93m" "\e[97m")

tput civis
trap 'tput cnorm; tput sgr0; clear; exit' INT TERM

init() {
    for ((i=0; i<COLS*ROWS; i++)); do
        grid[$i]=0
    done
}

render() {
    while true; do
        # Set bottom row to random heat
        for ((x=0; x<COLS; x++)); do
            grid[$((x + (ROWS-1)*COLS))]=$((RANDOM % 9))
        done

        # Propagate fire upward
        for ((y=ROWS-1; y>0; y--)); do
            for ((x=0; x<COLS; x++)); do
                decay=$((RANDOM % 3))
                src=$(( ((y)*COLS) + ((x + RANDOM%3 - 1 + COLS) % COLS) ))
                dst=$(( (y-1)*COLS + x ))
                val=$(( grid[src] - decay ))
                grid[$dst]=$(( val < 0 ? 0 : val ))
            done
        done

        # Draw
        local buf=""
        buf+="\e[H"
        for ((y=0; y<ROWS; y++)); do
            for ((x=0; x<COLS; x++)); do
                val=${grid[$((y*COLS + x))]}
                ci=$((val * ${#colors[@]} / 9))
                [[ $ci -ge ${#colors[@]} ]] && ci=$((${#colors[@]}-1))
                buf+="${colors[$ci]}${chars[$val]}"
            done
        done
        printf "$buf"
        sleep 0.05
    done
}

clear
init
render