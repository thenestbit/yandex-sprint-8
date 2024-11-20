package main

import (
	"database/sql"
	"fmt"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	query := `INSERT INTO parcel (client, status, address, created_at) VALUES (?, ?, ?, ?)`
	result, err := s.db.Exec(query, p.Client, p.Status, p.Address, p.CreatedAt)
	if err != nil {
		return 0, fmt.Errorf("failed to add parcel: %w", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve last insert id: %w", err)
	}

	return int(id), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	query := `SELECT number, client, status, address, created_at FROM parcel WHERE number = ?`
	row := s.db.QueryRow(query, number)

	p := Parcel{}
	err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return p, fmt.Errorf("parcel with number %d not found", number)
		}
		return p, fmt.Errorf("failed to get parcel: %w", err)
	}

	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	query := `SELECT number, client, status, address, created_at FROM parcel WHERE client = ?`
	rows, err := s.db.Query(query, client)
	if err != nil {
		return nil, fmt.Errorf("failed to get parcels for client %d: %w", client, err)
	}
	defer rows.Close()

	var parcels []Parcel
	for rows.Next() {
		p := Parcel{}
		err := rows.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("failed to scan parcel: %w", err)
		}
		parcels = append(parcels, p)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return parcels, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	query := `UPDATE parcel SET status = ? WHERE number = ?`
	_, err := s.db.Exec(query, status, number)
	if err != nil {
		return fmt.Errorf("failed to update status for parcel %d: %w", number, err)
	}

	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	// Сначала проверяем, что статус посылки - "registered"
	parcel, err := s.Get(number)
	if err != nil {
		return err
	}

	if parcel.Status != ParcelStatusRegistered {
		return fmt.Errorf("cannot change address for parcel %d as it is not in 'registered' status", number)
	}

	query := `UPDATE parcel SET address = ? WHERE number = ?`
	_, err = s.db.Exec(query, address, number)
	if err != nil {
		return fmt.Errorf("failed to update address for parcel %d: %w", number, err)
	}

	return nil
}

func (s ParcelStore) Delete(number int) error {
	// Сначала проверяем, что статус посылки - "registered"
	parcel, err := s.Get(number)
	if err != nil {
		return err
	}

	if parcel.Status != ParcelStatusRegistered {
		return fmt.Errorf("cannot delete parcel %d as it is not in 'registered' status", number)
	}

	query := `DELETE FROM parcel WHERE number = ?`
	_, err = s.db.Exec(query, number)
	if err != nil {
		return fmt.Errorf("failed to delete parcel %d: %w", number, err)
	}

	return nil
}
