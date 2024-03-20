\set scale {{ scale }}
\set ntellers 10 * :scale
\set naccounts 100000 * :scale
\set aid    random(1,:naccounts)
\set bid    random(1,:nbranches)
\set tid    random(1,:ntellers)
\set delta  random(-5000,5000)
{% if mixed %}
\set choice random(-100,100)
{% endif %}
BEGIN;
{% if mixed %}
{% for query in queries %}
{% if loop.first %}
\if :choice <= {{ query.percent }}
{% else %}
\elif :choice <= {{ query.percent }}
{% endif %}
{{ query.line }}
{% if loop.last %}
\endif
{% endif %}
{% endfor %}
{% else %}
{% for query in queries %}
{{ query.line }}
{% endfor %}
{% endif %}
END;
