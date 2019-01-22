# gocate - a cleverer (eventually) replacement for locate written in Golang

## Features

* Stores hash of file contents for later comparing/printing of duplicate files
* Can update the database starting at a specific directory in the file tree
*

## Usage

#### Update gocate database starting at /
`gocate -updatedb -path /`

#### Find files ending with certain characters
`gocate .md$`

#### Other
`gocate -updatedb -path /media/movies`

`gocate -updatedb -path ~/Music`
