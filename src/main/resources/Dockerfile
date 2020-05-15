FROM postgres
ENV POSTGRES_USER pokemondb
ENV POSTGRES_PASSWORD pokemondb
ADD schema.sql /docker-entrypoint-initdb.d
EXPOSE 5432