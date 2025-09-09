-- SETUP DO BANCO CÉREBRO v2.0
-- Execute este SQL no seu PostgreSQL para adicionar a tabela de contatos

-- Criar tabela de contatos (adiciona ao seu banco existente)
CREATE TABLE IF NOT EXISTS contacts (
    id SERIAL PRIMARY KEY,
    phone VARCHAR(20) NOT NULL,           -- Telefone normalizado  
    name VARCHAR(10) NOT NULL,            -- Data "09/09"
    instance VARCHAR(10) NOT NULL,        -- "GABY01", "GABY02", etc
    product VARCHAR(10),                  -- "FAB", "NAT", "CS"  
    conversation_id INTEGER,              -- Link com conversa se precisar
    saved_at TIMESTAMP DEFAULT NOW(),    -- Quando foi salvo
    created_at TIMESTAMP DEFAULT NOW(),
    
    -- Evitar duplicata do mesmo número no mesmo dia
    CONSTRAINT unique_phone_date UNIQUE(phone, name)
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_contacts_phone ON contacts(phone);
CREATE INDEX IF NOT EXISTS idx_contacts_instance ON contacts(instance);
CREATE INDEX IF NOT EXISTS idx_contacts_saved_at ON contacts(saved_at);
CREATE INDEX IF NOT EXISTS idx_contacts_name ON contacts(name);

-- Verificar se foi criada
SELECT 'Tabela contacts criada com sucesso!' as status;

-- Mostrar estrutura
\d contacts;
