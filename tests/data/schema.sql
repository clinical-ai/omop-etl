CREATE SCHEMA cerner;
CREATE SCHEMA omop;
CREATE SCHEMA mapping;
CREATE SCHEMA external;

create table omop.baz (id INTEGER, alpha varchar, beta integer, gamma integer, primary key (id));
create table omop.person (id INTEGER, name varchar, primary key (id));
create table omop.events (id INTEGER, staff_id INTEGER, patient_id INTEGER, primary key (id));

create table external.vocabulary  (id INTEGER, name varchar, primary key (id));

insert into external.vocabulary (id, name) values (0, 'vocab1');
insert into external.vocabulary (id, name) values (1, 'vocab2');
insert into external.vocabulary (id, name) values (2, 'vocab3');


SET search_path TO cerner;

create table foo (id INTEGER, alpha varchar, beta integer, gamma integer, primary key (id));
create table bar (id INTEGER, alpha varchar, beta integer, gamma integer, primary key (id));
create table foo2bar (foo_id INTEGER, bar_id INTEGER, alpha varchar, primary key (foo_id, bar_id));
create table patient (id INTEGER, name varchar, primary key (id));
create table staff (id INTEGER, name varchar, primary key (id));
create table event (id INTEGER, staff_id INTEGER, patient_id INTEGER, primary key (id));

insert into foo (id, alpha, beta, gamma) values (0, 'a', 4, 2);
insert into foo (id, alpha, beta, gamma) values (1, 'c', 5, 5);
insert into foo (id, alpha, beta, gamma) values (2, 'd', 9, 7);

insert into bar (id, alpha, beta, gamma) values (0, 'x', 8, 3);
insert into bar (id, alpha, beta, gamma) values (1, 'a', 4, 4);
insert into bar (id, alpha, beta, gamma) values (2, 'c', 6, 5);

insert into patient (id, name) values (100, 'alpha');
insert into patient (id, name) values (456, 'beta');
insert into patient (id, name) values (749, 'gamma');

insert into staff (id, name) values (101, 'one');
insert into staff (id, name) values (456, 'two');
insert into staff (id, name) values (457, 'three');

insert into event (id, staff_id, patient_id) values (0, 456, 456);
insert into event (id, staff_id, patient_id) values (2, 457, 456);
insert into event (id, staff_id, patient_id) values (3, 101, 100);
insert into event (id, staff_id, patient_id) values (4, NULL, 999);

insert into foo2bar (foo_id, bar_id) values (0, 1);
insert into foo2bar (foo_id, bar_id) values (1, 2);